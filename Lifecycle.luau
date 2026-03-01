--!strict

local Signal = require(script.Parent.Signal)
local Types = require(script.Parent.Types)

type Signal<T...> = Types.Signal<T...>

local Lifecycle = {}

type Closeable = {
	CloseAll : (self : any) -> (),
	_ShuttingDown : boolean,
	_AutoSaveThread : thread?,
}

local RegisteredStores : { Closeable } = {}
local IsBound = false
local IsShuttingDown = false

Lifecycle.OnShutdown = Signal.New() :: Signal<>

function Lifecycle.Register(StoreInstance : any) : ()
	table.insert(RegisteredStores, StoreInstance :: Closeable)
	Lifecycle._EnsureBound()
end

function Lifecycle.Unregister(StoreInstance : any) : ()
	local Index = table.find(RegisteredStores, StoreInstance :: Closeable)
	if Index then
		table.remove(RegisteredStores, Index)
	end
end

function Lifecycle.IsShuttingDown() : boolean
	return IsShuttingDown
end

function Lifecycle.GetRegisteredCount() : number
	return #RegisteredStores
end

function Lifecycle._EnsureBound() : ()
	if IsBound then
		return
	end
	IsBound = true

	game:BindToClose(function()
		IsShuttingDown = true


		for _, StoreInstance in RegisteredStores do
			StoreInstance._ShuttingDown = true

			if StoreInstance._AutoSaveThread then
				task.cancel(StoreInstance._AutoSaveThread)
				StoreInstance._AutoSaveThread = nil
			end
		end

		Lifecycle.OnShutdown:Fire()


		local Snapshot = table.clone(RegisteredStores)
		if #Snapshot == 0 then
			return
		end
		local RemainingCount = #Snapshot
		local CompletionSignal = Instance.new("BindableEvent")

		for _, StoreInstance in Snapshot do
			task.spawn(function()
				local Success, ErrorMessage = pcall(function() : string?
					StoreInstance:CloseAll()
					return nil
				end)
				if not Success then
					warn(`[Nexus] Lifecycle: error closing store: {ErrorMessage}`)
				end
				RemainingCount -= 1
				if RemainingCount == 0 then
					CompletionSignal:Fire()
				end
			end)
		end

		if RemainingCount > 0 then
			CompletionSignal.Event:Wait()
		end
		CompletionSignal:Destroy()
	end)
end

return Lifecycle
