--!strict

local Types = require(script.Parent.Types)

type MiddlewarePhase = Types.MiddlewarePhase

export type MiddlewareFn = (Key : string, Data : unknown, Next : () -> unknown) -> unknown

export type MiddlewarePipeline = {
	_LoadChain : { MiddlewareFn },
	_SaveChain : { MiddlewareFn },
	Use : (self : MiddlewarePipeline, Phase : MiddlewarePhase, Handler : MiddlewareFn) -> (),
	Run : (self : MiddlewarePipeline, Phase : MiddlewarePhase, Key : string, Data : unknown) -> unknown,
	HasMiddleware : (self : MiddlewarePipeline, Phase : MiddlewarePhase) -> boolean,
}

local Middleware = {}

function Middleware.New() : MiddlewarePipeline
	local Pipeline = {} :: any
	Pipeline._LoadChain = {}
	Pipeline._SaveChain = {}

	Pipeline.Use = function(self : MiddlewarePipeline, Phase : MiddlewarePhase, Handler : MiddlewareFn)
		if Phase == "load" then
			table.insert(self._LoadChain, Handler)
		elseif Phase == "save" then
			table.insert(self._SaveChain, Handler)
		end
	end

	Pipeline.Run = function(self : MiddlewarePipeline, Phase : MiddlewarePhase, Key : string, Data : unknown) : unknown
		local Chain = if Phase == "load" then self._LoadChain else self._SaveChain
		if #Chain == 0 then return Data end

		local CurrentData = Data
		local ChainIndex = 0

		local function RunNext() : unknown
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

	Pipeline.HasMiddleware = function(self : MiddlewarePipeline, Phase : MiddlewarePhase) : boolean
		if Phase == "load" then
			return #self._LoadChain > 0
		end
		return #self._SaveChain > 0
	end

	return Pipeline
end

return Middleware
