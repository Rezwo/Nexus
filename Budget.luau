--!strict
local DataStoreService = game:GetService("DataStoreService")

local Errors = require(script.Parent.Errors)
local Types = require(script.Parent.Types)

type NexusError = Types.NexusError
type Result<Value> = Types.Result<Value>

local Budget = {}

local LOW_BUDGET_THRESHOLD = 5
local DRAIN_INTERVAL = 0.5
local MAXIMUM_QUEUE_WAIT = 60

local LEGACY_REQUEST_TYPE_MAP : { [Types.BudgetType] : Enum.DataStoreRequestType } = {
	GetAsync = Enum.DataStoreRequestType.GetAsync,
	SetIncrementAsync = Enum.DataStoreRequestType.SetIncrementAsync,
	UpdateAsync = Enum.DataStoreRequestType.UpdateAsync,
	GetSortedAsync = Enum.DataStoreRequestType.GetSortedAsync,
	SetIncrementSortedAsync = Enum.DataStoreRequestType.SetIncrementSortedAsync,
	OnUpdate = Enum.DataStoreRequestType.OnUpdate,
	GetVersionAsync = Enum.DataStoreRequestType.GetVersionAsync,
	ListAsync = Enum.DataStoreRequestType.ListAsync,
}

local MODERN_REQUEST_TYPE_MAP : { [Types.BudgetType] : { Enum.DataStoreRequestType } } = {
	GetAsync = { Enum.DataStoreRequestType.StandardRead },
	SetIncrementAsync = { Enum.DataStoreRequestType.StandardWrite },
	UpdateAsync = { Enum.DataStoreRequestType.StandardRead, Enum.DataStoreRequestType.StandardWrite },
	GetSortedAsync = { Enum.DataStoreRequestType.OrderedList },
	SetIncrementSortedAsync = { Enum.DataStoreRequestType.OrderedWrite },
	OnUpdate = { Enum.DataStoreRequestType.StandardRead },
	GetVersionAsync = { Enum.DataStoreRequestType.StandardRead },
	ListAsync = { Enum.DataStoreRequestType.StandardList },
}

local UseModernBudgets : boolean = false
local DetectionDone : boolean = false

local function DetectBudgetSystem() : ()
	if DetectionDone then return end
	DetectionDone = true
	local ModernBudget = DataStoreService:GetRequestBudgetForRequestType(
		Enum.DataStoreRequestType.StandardRead
	)
	UseModernBudgets = ModernBudget > 0
end

type QueuedOperation = {
	Thread : thread,
	EnqueuedAt : number,
}

local OperationQueues : { [Types.BudgetType] : { QueuedOperation } } = {}
local DrainerRunning = false

function Budget.Get(RequestBudgetType : Types.BudgetType) : number
	DetectBudgetSystem()

	if UseModernBudgets then
		local ModernEnums = MODERN_REQUEST_TYPE_MAP[RequestBudgetType]
		if not ModernEnums then return 0 end
		local MinBudget = math.huge
		for _, EnumValue in ModernEnums do
			local CurrentBudget = DataStoreService:GetRequestBudgetForRequestType(EnumValue)
			if CurrentBudget < MinBudget then
				MinBudget = CurrentBudget
			end
		end
		return if MinBudget == math.huge then 0 else MinBudget
	end

	local LegacyEnum = LEGACY_REQUEST_TYPE_MAP[RequestBudgetType]
	if not LegacyEnum then return 0 end
	return DataStoreService:GetRequestBudgetForRequestType(LegacyEnum)
end

function Budget.HasCapacity(RequestBudgetType : Types.BudgetType) : boolean
	return Budget.Get(RequestBudgetType) > LOW_BUDGET_THRESHOLD
end

function Budget.WaitForCapacity(RequestBudgetType : Types.BudgetType) : Result<boolean>
	if Budget.HasCapacity(RequestBudgetType) then
		return Errors.Passed(true)
	end

	if not OperationQueues[RequestBudgetType] then
		OperationQueues[RequestBudgetType] = {}
	end

	local Entry : QueuedOperation = {
		Thread = coroutine.running(),
		EnqueuedAt = os.clock(),
	}
	table.insert(OperationQueues[RequestBudgetType], Entry)

	Budget._EnsureDrainer()

	local ResumedWithSuccess : boolean = coroutine.yield()

	if ResumedWithSuccess then
		return Errors.Passed(true)
	else
		return Errors.Fail(Errors.New("BudgetExhausted",
			`Budget for {RequestBudgetType} not available after {MAXIMUM_QUEUE_WAIT}s`
		))
	end
end

function Budget.WithBudget<Value>(RequestBudgetType : Types.BudgetType, Operation : () -> Value) : Result<Value>
	local WaitResult = Budget.WaitForCapacity(RequestBudgetType)
	if not WaitResult.Passed then
		return WaitResult :: any
	end

	local Success, OperationResult = pcall(Operation)
	if Success then
		return Errors.Passed(OperationResult)
	end
	return Errors.FromPcall("DataStoreError", false, OperationResult :: any)
end

function Budget.GetQueueInfo() : { [Types.BudgetType] : { QueueDepth : number, CurrentBudget : number } }
	local Information : { [Types.BudgetType] : { QueueDepth : number, CurrentBudget : number } } = {}
	for RequestBudgetType, _ in LEGACY_REQUEST_TYPE_MAP do
		local Queue = OperationQueues[RequestBudgetType]
		Information[RequestBudgetType] = {
			QueueDepth = if Queue then #Queue else 0,
			CurrentBudget = Budget.Get(RequestBudgetType),
		}
	end
	return Information
end

function Budget.IsModernBudgetActive() : boolean
	DetectBudgetSystem()
	return UseModernBudgets
end

function Budget.RedetectBudgetSystem() : ()
	DetectionDone = false
	DetectBudgetSystem()
end

function Budget._EnsureDrainer() : ()
	if DrainerRunning then
		return
	end
	DrainerRunning = true

	task.spawn(function()
		while true do
			task.wait(DRAIN_INTERVAL)

			local AnyPending = false
			local CurrentTime = os.clock()

			for RequestBudgetType, Queue in OperationQueues do
				if #Queue == 0 then
					continue
				end
				AnyPending = true

				while #Queue > 0 and Budget.HasCapacity(RequestBudgetType) do
					local Entry = table.remove(Queue, 1) :: QueuedOperation
					if CurrentTime - Entry.EnqueuedAt > MAXIMUM_QUEUE_WAIT then
						task.spawn(Entry.Thread, false)
					else
						task.spawn(Entry.Thread, true)
					end
				end

				local Index = 1
				while Index <= #Queue do
					if CurrentTime - Queue[Index].EnqueuedAt > MAXIMUM_QUEUE_WAIT then
						local TimedOutEntry = table.remove(Queue, Index) :: QueuedOperation
						task.spawn(TimedOutEntry.Thread, false)
					else
						Index += 1
					end
				end
			end

			if not AnyPending then
				DrainerRunning = false
				break
			end
		end
	end)
end

return Budget
