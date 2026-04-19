--!strict

local Semaphore = {}
Semaphore.__index = Semaphore

export type SemaphoreObject = typeof(setmetatable({} :: {
	_MaxConcurrent : number,
	_CurrentCount : number,
	_WaitingQueue : { thread },
	_Destroyed : boolean,
}, {} :: { __index : typeof(Semaphore) }))

function Semaphore.New(MaxConcurrent : number) : SemaphoreObject
	assert(typeof(MaxConcurrent) == "number", "MaxConcurrent must be a number")
	assert(MaxConcurrent >= 1, "Semaphore must allow at least 1 concurrent operation")
	assert(MaxConcurrent == math.floor(MaxConcurrent), "MaxConcurrent must be an integer")
	local self = setmetatable({}, Semaphore)
	self._MaxConcurrent = MaxConcurrent
	self._CurrentCount = 0
	self._WaitingQueue = {}
	self._Destroyed = false
	return self :: any
end

function Semaphore.Acquire(self : SemaphoreObject) : ()
	if self._Destroyed then
		error("Semaphore is destroyed", 2)
	end

	-- Luau is cooperative; the check-and-increment below cannot be preempted
	-- because no yield happens between them.
	if self._CurrentCount < self._MaxConcurrent then
		self._CurrentCount += 1
		return
	end

	table.insert(self._WaitingQueue, coroutine.running())
	coroutine.yield()

	if self._Destroyed then
		error("Semaphore was destroyed while waiting", 2)
	end
end

function Semaphore.Release(self : SemaphoreObject) : ()
	if self._Destroyed then
		return
	end

	-- Wake a waiter if any: ownership of the slot transfers without touching
	-- _CurrentCount (the released slot is immediately claimed).
	if #self._WaitingQueue > 0 then
		local NextThread = table.remove(self._WaitingQueue, 1) :: thread
		task.spawn(NextThread)
		return
	end

	if self._CurrentCount > 0 then
		self._CurrentCount -= 1
	else
		-- Unbalanced release — treat as a programmer error but don't let counts go negative.
		warn("[Nexus] Semaphore.Release called without matching Acquire")
	end
end

function Semaphore.WithSlot<T>(self : SemaphoreObject, Operation : () -> T) : T
	self:Acquire()
	local Success, Result = xpcall(Operation, function(ErrorValue)
		return debug.traceback(tostring(ErrorValue), 2)
	end)
	self:Release()
	if not Success then
		error(Result :: any, 2)
	end
	return Result :: T
end

function Semaphore.GetInfo(self : SemaphoreObject) : {
	MaxConcurrent : number,
	ActiveCount : number,
	QueueDepth : number,
}
	return {
		MaxConcurrent = self._MaxConcurrent,
		ActiveCount = self._CurrentCount,
		QueueDepth = #self._WaitingQueue,
	}
end

function Semaphore.Destroy(self : SemaphoreObject) : ()
	if self._Destroyed then return end
	self._Destroyed = true

	-- Resume any stranded waiters so they can exit cleanly.
	local WaitingQueue = self._WaitingQueue
	self._WaitingQueue = {}
	for _, WaitingThread in WaitingQueue do
		task.spawn(WaitingThread)
	end
end

local GlobalWriteSemaphore = Semaphore.New(10)

Semaphore.Global = GlobalWriteSemaphore

return Semaphore
