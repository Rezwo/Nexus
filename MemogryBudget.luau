--!strict

local MemoryStoreService = game:GetService("MemoryStoreService")

local Errors = require(script.Parent.Errors)
local Types = require(script.Parent.Types)

type Result<Value> = Types.Result<Value>

local MemoryBudget = {}

local LOW_BUDGET_THRESHOLD = 3
local DRAIN_INTERVAL = 0.5
local MAXIMUM_QUEUE_WAIT = 30

export type MemoryBudgetType =
	"SortedMap"
	| "HashMap"
	| "Queue"

local REQUEST_TYPE_MAP : { [MemoryBudgetType] : Enum.DataStoreRequestType } = {
	SortedMap = Enum.DataStoreRequestType.SetIncrementAsync,
	HashMap = Enum.DataStoreRequestType.SetIncrementAsync,
	Queue = Enum.DataStoreRequestType.SetIncrementAsync,
}

type QueuedOperation = {
	Thread : thread,
	EnqueuedAt : number,
}

local OperationQueues : { [MemoryBudgetType] : { QueuedOperation } } = {}
local DrainerRunning = false

local RecentFailures : { number } = {}
local FAILURE_WINDOW = 10
local FAILURE_THRESHOLD = 5
local BACKOFF_DURATION = 5
local BackoffUntil = 0

local function RecordFailure() : ()
	table.insert(RecentFailures, os.clock())
end

local function IsInBackoff() : boolean
	if os.clock() < BackoffUntil then
		return true
	end
	local Cutoff = os.clock() - FAILURE_WINDOW
	local ActiveFailures = 0
	local WriteIndex = 1
	for ReadIndex = 1, #RecentFailures do
		if RecentFailures[ReadIndex] >= Cutoff then
			RecentFailures[WriteIndex] = RecentFailures[ReadIndex]
			WriteIndex += 1
			ActiveFailures += 1
		end
	end
	for ClearIndex = WriteIndex, #RecentFailures do
		RecentFailures[ClearIndex] = nil :: any
	end
	if ActiveFailures >= FAILURE_THRESHOLD then
		BackoffUntil = os.clock() + BACKOFF_DURATION
		return true
	end
	return false
end

function MemoryBudget.ShouldAttempt() : boolean
	return not IsInBackoff()
end

function MemoryBudget.RecordFailure() : ()
	RecordFailure()
end

function MemoryBudget.RecordSuccess() : ()
	-- intentionally empty
end

function MemoryBudget.HasCapacity(RequestBudgetType : MemoryBudgetType) : boolean
	if IsInBackoff() then return false end
	return true
end

function MemoryBudget.WaitForCapacity(RequestBudgetType : MemoryBudgetType) : Result<boolean>
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
