--!strict

local Errors = require(script.Parent.Errors)
local Types = require(script.Parent.Types)

type Result<Value> = Types.Result<Value>

local DRAIN_INTERVAL = 0.5
local MAXIMUM_QUEUE_WAIT = 30
local FAILURE_WINDOW = 10
local FAILURE_THRESHOLD = 5
local BACKOFF_DURATION = 5

type QueuedOperation = {
	Thread : thread,
	EnqueuedAt : number,
}

type ResourceBucket = {
	Failures : { number },
	BackoffUntil : number,
	Queues : { [string] : { QueuedOperation } },
	Draining : boolean,
}

local Buckets : { [string] : ResourceBucket } = {}

local function GetBucket(Resource : string) : ResourceBucket
	local ExistingBucket = Buckets[Resource]
	if ExistingBucket then return ExistingBucket end
	local NewBucket : ResourceBucket = {
		Failures = {},
		BackoffUntil = 0,
		Queues = {},
		Draining = false,
	}
	Buckets[Resource] = NewBucket
	return NewBucket
end

local function PruneFailures(Bucket : ResourceBucket) : number
	local Cutoff = os.clock() - FAILURE_WINDOW
	local WriteIndex = 1
	local ActiveCount = 0
	for ReadIndex = 1, #Bucket.Failures do
		if Bucket.Failures[ReadIndex] >= Cutoff then
			Bucket.Failures[WriteIndex] = Bucket.Failures[ReadIndex]
			WriteIndex += 1
			ActiveCount += 1
		end
	end
	for ClearIndex = WriteIndex, #Bucket.Failures do
		Bucket.Failures[ClearIndex] = nil :: any
	end
	return ActiveCount
end

local function IsInBackoff(Bucket : ResourceBucket) : boolean
	if os.clock() < Bucket.BackoffUntil then return true end
	local ActiveFailures = PruneFailures(Bucket)
	if ActiveFailures >= FAILURE_THRESHOLD then
		Bucket.BackoffUntil = os.clock() + BACKOFF_DURATION
		return true
	end
	return false
end


local MemoryBudget = {}

function MemoryBudget.RecordFailure(Resource : string) : ()
	local Bucket = GetBucket(Resource)
	table.insert(Bucket.Failures, os.clock())
end

function MemoryBudget.RecordSuccess(Resource : string) : ()
	local Bucket = GetBucket(Resource)
	if #Bucket.Failures > 0 then
		table.remove(Bucket.Failures, 1)
	end
end

function MemoryBudget.ShouldAttempt(Resource : string) : boolean
	return not IsInBackoff(GetBucket(Resource))
end

function MemoryBudget.HasCapacity(Resource : string) : boolean
	return not IsInBackoff(GetBucket(Resource))
end

function MemoryBudget.WaitForCapacity(Resource : string) : Result<boolean>
	local Bucket = GetBucket(Resource)
	if not IsInBackoff(Bucket) then return Errors.Passed(true) end

	if not Bucket.Queues["_default"] then Bucket.Queues["_default"] = {} end

	local Entry : QueuedOperation = {
		Thread = coroutine.running(),
		EnqueuedAt = os.clock(),
	}
	table.insert(Bucket.Queues["_default"], Entry)
	MemoryBudget._EnsureDrainer(Resource)

	local ResumedWithSuccess : boolean = coroutine.yield()
	if ResumedWithSuccess then
		return Errors.Passed(true)
	end
	return Errors.Fail(Errors.New("BudgetExhausted",
		`MemoryStore budget for '{Resource}' not available after {MAXIMUM_QUEUE_WAIT}s`))
end

function MemoryBudget.GetStatus(Resource : string) : {
	InBackoff : boolean,
	BackoffRemaining : number,
	RecentFailureCount : number,
	}
	local Bucket = GetBucket(Resource)
	local CurrentTime = os.clock()
	return {
		InBackoff = IsInBackoff(Bucket),
		BackoffRemaining = math.max(0, Bucket.BackoffUntil - CurrentTime),
		RecentFailureCount = #Bucket.Failures,
	}
end

function MemoryBudget.Reset(Resource : string?) : ()
	if Resource then
		local Bucket = Buckets[Resource]
		if Bucket then
			table.clear(Bucket.Failures)
			Bucket.BackoffUntil = 0
		end
	else

		table.clear(Buckets)
	end
end



function MemoryBudget._EnsureDrainer(Resource : string) : ()
	local Bucket = GetBucket(Resource)
	if Bucket.Draining then return end
	Bucket.Draining = true

	task.spawn(function()
		while true do
			task.wait(DRAIN_INTERVAL)
			local AnyPending = false
			local CurrentTime = os.clock()

			for _QueueKey, Queue in Bucket.Queues do
				if #Queue == 0 then continue end
				AnyPending = true

				while #Queue > 0 and not IsInBackoff(Bucket) do
					local QueueEntry = table.remove(Queue, 1) :: QueuedOperation
					if CurrentTime - QueueEntry.EnqueuedAt > MAXIMUM_QUEUE_WAIT then
						task.spawn(QueueEntry.Thread, false)
					else
						task.spawn(QueueEntry.Thread, true)
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
				Bucket.Draining = false
				break
			end
		end
	end)
end

return MemoryBudget
