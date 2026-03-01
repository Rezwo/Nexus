--!strict

local Semaphore = {}
Semaphore.__index = Semaphore

export type SemaphoreObject = typeof(setmetatable({} :: {
	_MaxConcurrent : number,
	_CurrentCount : number,
	_WaitingQueue : { thread },
}, {} :: { __index : typeof(Semaphore) }))

function Semaphore.New(MaxConcurrent : number) : SemaphoreObject
	assert(MaxConcurrent >= 1, "Semaphore must allow at least 1 concurrent operation")
	local self = setmetatable({}, Semaphore)
	self._MaxConcurrent = MaxConcurrent
	self._CurrentCount = 0
	self._WaitingQueue = {}
	return self :: any
end

function Semaphore.Acquire(self : SemaphoreObject) : ()
	if self._CurrentCount < self._MaxConcurrent then
		self._CurrentCount += 1
		return
	end


	table.insert(self._WaitingQueue, coroutine.running())
	coroutine.yield()

end

function Semaphore.Release(self : SemaphoreObject) : ()
	if #self._WaitingQueue > 0 then

		local NextThread = table.remove(self._WaitingQueue, 1) :: thread
		task.spawn(NextThread)
	else
		self._CurrentCount -= 1
	end
end

function Semaphore.WithSlot<T>(self : SemaphoreObject, Operation : () -> T) : T
	self:Acquire()
	local Success, Result = pcall(Operation)
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

local GlobalWriteSemaphore = Semaphore.New(10)

Semaphore.Global = GlobalWriteSemaphore

return Semaphore
