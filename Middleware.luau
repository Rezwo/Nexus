--!strict

local Types = require(script.Parent.Types)

type MiddlewarePhase = Types.MiddlewarePhase

export type MiddlewareFn = (Key : string, Data : unknown, Next : () -> unknown) -> unknown

export type MiddlewarePipeline = {
	_LoadChain : { MiddlewareFn },
	_SaveChain : { MiddlewareFn },
	_MaxDepth : number,
	Use : (self : MiddlewarePipeline, Phase : MiddlewarePhase, Handler : MiddlewareFn) -> (),
	Run : (self : MiddlewarePipeline, Phase : MiddlewarePhase, Key : string, Data : unknown) -> unknown,
	RunSafe : (self : MiddlewarePipeline, Phase : MiddlewarePhase, Key : string, Data : unknown) -> (boolean, unknown),
	HasMiddleware : (self : MiddlewarePipeline, Phase : MiddlewarePhase) -> boolean,
}

local Middleware = {}

local DEFAULT_MAX_DEPTH = 256

function Middleware.New() : MiddlewarePipeline
	local Pipeline = {} :: any
	Pipeline._LoadChain = {}
	Pipeline._SaveChain = {}
	Pipeline._MaxDepth = DEFAULT_MAX_DEPTH

	Pipeline.Use = function(self : MiddlewarePipeline, Phase : MiddlewarePhase, Handler : MiddlewareFn)
		assert(typeof(Handler) == "function", "Middleware handler must be a function")
		if Phase == "load" then
			table.insert(self._LoadChain, Handler)
		elseif Phase == "save" then
			table.insert(self._SaveChain, Handler)
		else
			error(`Unknown middleware phase '{tostring(Phase)}' (expected "load" or "save")`, 2)
		end
	end

	Pipeline.Run = function(self : MiddlewarePipeline, Phase : MiddlewarePhase, Key : string, Data : unknown) : unknown
		local Chain = if Phase == "load" then self._LoadChain else self._SaveChain
		if #Chain == 0 then return Data end

		local CurrentData = Data
		local ChainIndex = 0
		local NextCallDepth = 0
		local MaxDepth = self._MaxDepth

		local RunNext : (... any) -> unknown
		RunNext = function(... : any) : unknown
			NextCallDepth += 1
			if NextCallDepth > MaxDepth then
				error(`Middleware pipeline exceeded maximum depth of {MaxDepth} — likely recursive Next() calls`, 2)
			end
			local Args = { ... }
			if #Args >= 2 then
				CurrentData = Args[2]
			elseif #Args == 1 and typeof(Args[1]) ~= "string" then
				CurrentData = Args[1]
			end
			ChainIndex += 1
			if ChainIndex > #Chain then
				return CurrentData
			end
			local Handler = Chain[ChainIndex]
			local Result = Handler(Key, CurrentData, RunNext)
			if Result ~= nil then
				CurrentData = Result
			end
			return CurrentData
		end

		return RunNext()
	end

	Pipeline.RunSafe = function(self : MiddlewarePipeline, Phase : MiddlewarePhase, Key : string, Data : unknown) : (boolean, unknown)
		-- Isolate middleware errors from the host call path. Store uses this
		-- so a misbehaving handler surfaces as an error result rather than an
		-- uncaught throw through load/save.
		local Success, ResultOrError = pcall(function()
			return self:Run(Phase, Key, Data)
		end)
		if Success then
			return true, ResultOrError
		end
		return false, ResultOrError
	end

	Pipeline.HasMiddleware = function(self : MiddlewarePipeline, Phase : MiddlewarePhase) : boolean
		if Phase == "load" then
			return #self._LoadChain > 0
		end
		return #self._SaveChain > 0
	end

	return Pipeline
end

return Middleware
