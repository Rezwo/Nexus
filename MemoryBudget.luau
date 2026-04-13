--!strict

local Errors = require(script.Parent.Errors)
local Types = require(script.Parent.Types)

type Result<Value> = Types.Result<Value>
type BudgetType = Types.BudgetType

local MemoryBudget = {}

local DRAIN_INTERVAL = 0.5
local MAXIMUM_QUEUE_WAIT = 30
local FAILURE_WINDOW = 10
local FAILURE_THRESHOLD = 5
local BACKOFF_DURATION = 5

type QueuedOperation = {
	Thread : thread,
	EnqueuedAt : number,
}

local OperationQueues : { [BudgetType] : { QueuedOperation } } = {}
local DrainerRunning = false
local RecentFailures : { number } = {}
local BackoffUntil = 0

local function PruneFailures() : number
	local Cutoff = os.clock() - FAILURE_WINDOW
	local ActiveCount = 0
	local WriteIndex = 1
	for ReadIndex = 1, #RecentFailures do
		if RecentFailures[ReadIndex] >= Cutoff then
			RecentFailures[WriteIndex] = RecentFailures[ReadIndex]
			WriteIndex += 1
			ActiveCount += 1
		end
	end
	for ClearIndex = WriteIndex, #RecentFailures do
		RecentFailures[ClearIndex] = nil :: any
	end
	return ActiveCount
end

local function IsInBackoff() : boolean
	if os.clock() < BackoffUntil then return true end
	local ActiveFailures = PruneFailures()
	if ActiveFailures >= FAILURE_THRESHOLD then
		BackoffUntil = os.clock() + BACKOFF_DURATION
		return true
	end
	return false
end

function MemoryBudget.Get(RequestBudgetType : BudgetType) : number
	if IsInBackoff() then return 0 end
	return FAILURE_THRESHOLD - #RecentFailures
end

function MemoryBudget.ShouldAttempt() : boolean
	return not IsInBackoff()
end

function MemoryBudget.RecordFailure() : ()
	table.insert(RecentFailures, os.clock())
end

function MemoryBudget.RecordSuccess() : ()
end

function MemoryBudget.HasCapacity(RequestBudgetType : BudgetType) : boolean
	if IsInBackoff() then return false end
	return true
end

function MemoryBudget.WaitForCapacity(RequestBudgetType : BudgetType) : Result<boolean>
	if MemoryBudget.HasCapacity(RequestBudgetType) then
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

	MemoryBudget._EnsureDrainer()

	local ResumedWithSuccess : boolean = coroutine.yield()
	if ResumedWithSuccess then
		return Errors.Passed(true)
	else
		return Errors.Fail(Errors.New("BudgetExhausted",
			`MemoryStore budget for {RequestBudgetType} not available after {MAXIMUM_QUEUE_WAIT}s`
			))
	end
end

function MemoryBudget.GetQueueInfo() : { [BudgetType] : { QueueDepth : number, CurrentBudget : number } }
	local Information : { [BudgetType] : { QueueDepth : number, CurrentBudget : number } } = {}
	local AllTypes : { BudgetType } = {
		"GetAsync", "SetIncrementAsync", "UpdateAsync", "GetSortedAsync",
		"SetIncrementSortedAsync", "OnUpdate", "GetVersionAsync", "ListAsync",
	}
	for _, RequestBudgetType in AllTypes do
		local Queue = OperationQueues[RequestBudgetType]
		Information[RequestBudgetType] = {
			QueueDepth = if Queue then #Queue else 0,
			CurrentBudget = MemoryBudget.Get(RequestBudgetType),
		}
	end
	return Information
end

function MemoryBudget.GetStatus() : {
	InBackoff : boolean,
	BackoffRemaining : number,
	RecentFailureCount : number,
	}
	local Now = os.clock()
	return {
		InBackoff = IsInBackoff(),
		BackoffRemaining = math.max(0, BackoffUntil - Now),
		RecentFailureCount = #RecentFailures,
	}
end

function MemoryBudget.Reset() : ()
	table.clear(RecentFailures)
	BackoffUntil = 0
end

function MemoryBudget._EnsureDrainer() : ()
	if DrainerRunning then return end
	DrainerRunning = true

	task.spawn(function()
		while true do
			task.wait(DRAIN_INTERVAL)
			local AnyPending = false
			local CurrentTime = os.clock()

			for RequestBudgetType, Queue in OperationQueues do
				if #Queue == 0 then continue end
				AnyPending = true

				while #Queue > 0 and MemoryBudget.HasCapacity(RequestBudgetType) do
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
						local TimedOut = table.remove(Queue, Index) :: QueuedOperation
						task.spawn(TimedOut.Thread, false)
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

return MemoryBudget
