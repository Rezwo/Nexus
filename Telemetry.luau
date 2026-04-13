--!strict

local Types = require(script.Parent.Types)

type NexusError = Types.NexusError

local Telemetry = {}

export type OperationStats = {
	TotalCount : number,
	SuccessCount : number,
	FailureCount : number,
	TotalLatency : number,
	MinLatency : number,
	MaxLatency : number,
	MeanLatency : number,
	ErrorCounts : { [string] : number },
	LastErrorMessage : string?,
	LastOperationTime : number,
}

type InternalStats = {
	TotalCount : number,
	SuccessCount : number,
	FailureCount : number,
	TotalLatency : number,
	MinLatency : number,
	MaxLatency : number,
	ErrorCounts : { [string] : number },
	LastErrorMessage : string?,
	LastOperationTime : number,
}

local GlobalData : { [string] : InternalStats } = {}

local function EnsureStats(Pool : { [string] : InternalStats }, OperationName : string) : InternalStats
	local Existing = Pool[OperationName]
	if Existing then return Existing end
	local NewStats : InternalStats = {
		TotalCount = 0,
		SuccessCount = 0,
		FailureCount = 0,
		TotalLatency = 0,
		MinLatency = math.huge,
		MaxLatency = 0,
		ErrorCounts = {},
		LastErrorMessage = nil,
		LastOperationTime = 0,
	}
	Pool[OperationName] = NewStats
	return NewStats
end

local function RecordToPool(
	Pool : { [string] : InternalStats },
	OperationName : string,
	Latency : number,
	Success : boolean,
	ErrorInfo : NexusError?
) : ()
	local Stats = EnsureStats(Pool, OperationName)
	Stats.TotalCount += 1
	Stats.TotalLatency += Latency
	Stats.LastOperationTime = os.clock()
	if Latency < Stats.MinLatency then Stats.MinLatency = Latency end
	if Latency > Stats.MaxLatency then Stats.MaxLatency = Latency end
	if Success then
		Stats.SuccessCount += 1
	else
		Stats.FailureCount += 1
		if ErrorInfo then
			local Kind = ErrorInfo.Kind
			Stats.ErrorCounts[Kind] = (Stats.ErrorCounts[Kind] or 0) + 1
			Stats.LastErrorMessage = ErrorInfo.Message
		end
	end
end

local function StatsToPublic(Stats : InternalStats) : OperationStats
	return {
		TotalCount = Stats.TotalCount,
		SuccessCount = Stats.SuccessCount,
		FailureCount = Stats.FailureCount,
		TotalLatency = Stats.TotalLatency,
		MinLatency = if Stats.MinLatency == math.huge then 0 else Stats.MinLatency,
		MaxLatency = Stats.MaxLatency,
		MeanLatency = if Stats.TotalCount > 0 then Stats.TotalLatency / Stats.TotalCount else 0,
		ErrorCounts = table.clone(Stats.ErrorCounts),
		LastErrorMessage = Stats.LastErrorMessage,
		LastOperationTime = Stats.LastOperationTime,
	}
end

export type TelemetryScope = {
	_Pool : { [string] : InternalStats },
	_Name : string,
	Record : (self : TelemetryScope, OperationName : string, Latency : number, Success : boolean, ErrorInfo : NexusError?) -> (),
	GetOperation : (self : TelemetryScope, OperationName : string) -> OperationStats?,
	GetReport : (self : TelemetryScope) -> { [string] : OperationStats },
	Reset : (self : TelemetryScope) -> (),
}

function Telemetry.CreateScope(Name : string) : TelemetryScope
	local Pool : { [string] : InternalStats } = {}
	local Scope = {} :: any
	Scope._Pool = Pool
	Scope._Name = Name

	Scope.Record = function(_self : TelemetryScope, OperationName : string, Latency : number, SuccessOrResult : any, ErrorInfo : NexusError?)
		local Success : boolean
		local ResolvedError : NexusError? = ErrorInfo
		if typeof(SuccessOrResult) == "table" and (SuccessOrResult :: any).Passed ~= nil then
			Success = (SuccessOrResult :: any).Passed == true
			if not Success and (SuccessOrResult :: any).Error then
				ResolvedError = (SuccessOrResult :: any).Error
			end
		else
			Success = if SuccessOrResult then true else false
		end
		RecordToPool(Pool, OperationName, Latency, Success, ResolvedError)
		RecordToPool(GlobalData, OperationName, Latency, Success, ResolvedError)
	end

	Scope.GetOperation = function(_self : TelemetryScope, OperationName : string) : OperationStats?
		local Stats = Pool[OperationName]
		if not Stats then return nil end
		return StatsToPublic(Stats)
	end

	Scope.GetReport = function(_self : TelemetryScope) : { [string] : OperationStats }
		local Report : { [string] : OperationStats } = {}
		for OpName, Stats in Pool do
			Report[OpName] = StatsToPublic(Stats)
		end
		return Report
	end

	Scope.Reset = function(_self : TelemetryScope)
		table.clear(Pool)
	end

	return Scope
end

function Telemetry.Record(
	OperationName : string,
	Latency : number,
	SuccessOrResult : any,
	ErrorInfo : NexusError?
) : ()
	local Success : boolean
	local ResolvedError : NexusError? = ErrorInfo
	if typeof(SuccessOrResult) == "table" and (SuccessOrResult :: any).Passed ~= nil then
		Success = (SuccessOrResult :: any).Passed == true
		if not Success and (SuccessOrResult :: any).Error then
			ResolvedError = (SuccessOrResult :: any).Error
		end
	else
		Success = if SuccessOrResult then true else false
	end
	RecordToPool(GlobalData, OperationName, Latency, Success, ResolvedError)
end

function Telemetry.GetOperation(OperationName : string) : OperationStats?
	local Stats = GlobalData[OperationName]
	if not Stats then return nil end
	return StatsToPublic(Stats)
end

function Telemetry.GetReport() : { [string] : OperationStats }
	local Report : { [string] : OperationStats } = {}
	for OperationName, Stats in GlobalData do
		Report[OperationName] = StatsToPublic(Stats)
	end
	return Report
end

function Telemetry.GetSummary() : {
	TotalOperations : number,
	TotalSuccesses : number,
	TotalFailures : number,
	OverallSuccessRate : number,
	TrackedOperations : number,
	}
	local TotalOps, TotalOk, TotalFail, OpCount = 0, 0, 0, 0
	for _, Stats in GlobalData do
		TotalOps += Stats.TotalCount
		TotalOk += Stats.SuccessCount
		TotalFail += Stats.FailureCount
		OpCount += 1
	end
	return {
		TotalOperations = TotalOps,
		TotalSuccesses = TotalOk,
		TotalFailures = TotalFail,
		OverallSuccessRate = if TotalOps > 0 then TotalOk / TotalOps else 1,
		TrackedOperations = OpCount,
	}
end

function Telemetry.GetTrackedOperations() : { string }
	local Names : { string } = {}
	for OperationName, _ in GlobalData do
		table.insert(Names, OperationName)
	end
	table.sort(Names)
	return Names
end

function Telemetry.Reset() : ()
	table.clear(GlobalData)
end

function Telemetry.ResetOperation(OperationName : string) : ()
	GlobalData[OperationName] = nil
end

return Telemetry
