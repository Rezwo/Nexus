--!strict

local Types = require(script.Parent.Types)

type Connection = Types.Connection
type Signal<T...> = Types.Signal<T...>

type CallbackEntry = {
	Callback : (...any) -> (),
	Once : boolean,
}

local Signal = {}
Signal.__index = Signal

function Signal.New<T...>() : Signal<T...>
	local self = setmetatable({}, Signal)
	self._Listeners = {} :: { CallbackEntry }
	self._Waiting = {} :: { thread }
	return self :: any
end

function Signal.Connect<T...>(self : Signal<T...>, Callback : (T...) -> ()) : Connection
	local Entry : CallbackEntry = { Callback = Callback :: any, Once = false }
	table.insert((self :: any)._Listeners, Entry)

	local SignalReference = self

	local ConnectionObject = {} :: any
	ConnectionObject.Connected = true
	ConnectionObject.Disconnect = function(ConnectionSelf : Connection)
		if not ConnectionObject.Connected then return end
		ConnectionObject.Connected = false
		local Listeners = (SignalReference :: any)._Listeners :: { CallbackEntry }
		local Index = table.find(Listeners, Entry)
		if Index then
			table.remove(Listeners, Index)
		end
	end
	return ConnectionObject :: Connection
end

function Signal.Once<T...>(self : Signal<T...>, Callback : (T...) -> ()) : Connection
	local Entry : CallbackEntry = { Callback = Callback :: any, Once = true }
	table.insert((self :: any)._Listeners, Entry)

	local SignalReference = self

	local ConnectionObject = {} :: any
	ConnectionObject.Connected = true
	ConnectionObject.Disconnect = function(ConnectionSelf : Connection)
		if not ConnectionObject.Connected then return end
		ConnectionObject.Connected = false
		local Listeners = (SignalReference :: any)._Listeners :: { CallbackEntry }
		local Index = table.find(Listeners, Entry)
		if Index then
			table.remove(Listeners, Index)
		end
	end
	return ConnectionObject :: Connection
end

function Signal.Fire<T...>(self : Signal<T...>, ... : T...)
	local Listeners = (self :: any)._Listeners :: { CallbackEntry }
	local Snapshot = table.clone(Listeners)
	local Index = 1
	while Index <= #Snapshot do
		local Entry = Snapshot[Index]
		task.spawn(Entry.Callback, ...)
		if Entry.Once then
			local RealIndex = table.find(Listeners, Entry)
			if RealIndex then
				table.remove(Listeners, RealIndex)
			end
		end
		Index += 1
	end

	local WaitingThreads = (self :: any)._Waiting :: { thread }
	for _, WaitingThread in WaitingThreads do
		task.spawn(WaitingThread, ...)
	end
	table.clear(WaitingThreads)
end

function Signal.Wait<T...>(self : Signal<T...>) : T...
	local WaitingThreads = (self :: any)._Waiting :: { thread }
	table.insert(WaitingThreads, coroutine.running())
	return coroutine.yield()
end

function Signal.Destroy<T...>(self : Signal<T...>)
	local Listeners = (self :: any)._Listeners :: { CallbackEntry }
	table.clear(Listeners)

	local WaitingThreads = (self :: any)._Waiting :: { thread }
	for _, WaitingThread in WaitingThreads do
		task.cancel(WaitingThread)
	end
	table.clear(WaitingThreads)
end

return Signal
