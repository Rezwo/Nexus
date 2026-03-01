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

local OperationData : { [string] : InternalStats } = {}

local function EnsureOperation(OperationName : string) : InternalStats
	local Existing = OperationData[OperationName]
	if Existing then
		return Existing
	end
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
	OperationData[OperationName] = NewStats
	return NewStats
end

function Telemetry.Record(
	OperationName : string,
	Latency : number,
	Success : boolean,
	ErrorInfo : NexusError?
) : ()
	local Stats = EnsureOperation(OperationName)

	Stats.TotalCount += 1
	Stats.TotalLatency += Latency
	Stats.LastOperationTime = os.clock()

	if Latency < Stats.MinLatency then
		Stats.MinLatency = Latency
	end
	if Latency > Stats.MaxLatency then
		Stats.MaxLatency = Latency
	end

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

function Telemetry.RecordResult(
	OperationName : string,
	Latency : number,
	OperationResult : { Passed : boolean }
) : ()
	if OperationResult.Passed then
		Telemetry.Record(OperationName, Latency, true)
	else
		Telemetry.Record(
			OperationName, Latency, false,
			(OperationResult :: any).Error :: NexusError?
		)
	end
end

function Telemetry.GetOperation(OperationName : string) : OperationStats?
	local Stats = OperationData[OperationName]
	if not Stats then
		return nil
	end
	return {
		TotalCount = Stats.TotalCount,
		SuccessCount = Stats.SuccessCount,
		FailureCount = Stats.FailureCount,
		TotalLatency = Stats.TotalLatency,
		MinLatency = if Stats.MinLatency == math.huge then 0 else Stats.MinLatency,
		MaxLatency = Stats.MaxLatency,
		MeanLatency = if Stats.TotalCount > 0
			then Stats.TotalLatency / Stats.TotalCount
			else 0,
		ErrorCounts = table.clone(Stats.ErrorCounts),
		LastErrorMessage = Stats.LastErrorMessage,
		LastOperationTime = Stats.LastOperationTime,
	}
end

function Telemetry.GetReport() : { [string] : OperationStats }
	local Report : { [string] : OperationStats } = {}
	for OperationName, _ in OperationData do
		Report[OperationName] = Telemetry.GetOperation(OperationName) :: OperationStats
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
	local TotalOps = 0
	local TotalOk = 0
	local TotalFail = 0
	local OpCount = 0

	for _, Stats in OperationData do
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
	for OperationName, _ in OperationData do
		table.insert(Names, OperationName)
	end
	table.sort(Names)
	return Names
end

function Telemetry.Reset() : ()
	table.clear(OperationData)
end

function Telemetry.ResetOperation(OperationName : string) : ()
	OperationData[OperationName] = nil
end

return Telemetry
