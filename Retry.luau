--!strict

local Errors = require(script.Parent.Errors)
local Budget = require(script.Parent.Budget)
local Types = require(script.Parent.Types)

type Result<Value> = Types.Result<Value>
type NexusError = Types.NexusError
type BudgetType = Types.BudgetType

local Retry = {}

local DEFAULT_MAXIMUM_RETRIES = 5
local BASE_DELAY = 1.0
local MAXIMUM_DELAY = 30.0

local function ComputeDelay(Attempt : number) : number
	local Exponential = BASE_DELAY * (2 ^ Attempt)
	local Jitter = math.random() * BASE_DELAY
	return math.min(Exponential + Jitter, MAXIMUM_DELAY)
end

export type RetryConfiguration = Types.RetryConfiguration

function Retry.Execute<Value>(
	Operation : () -> Result<Value>,
	Configuration : RetryConfiguration?
) : Result<Value>
	local Config : RetryConfiguration = Configuration or {}
	local MaximumRetries = Config.MaxRetries or DEFAULT_MAXIMUM_RETRIES
	local RequestBudgetType = Config.BudgetType
	local OnRetryCallback = Config.OnRetry

	local LastError : NexusError? = nil

	for Attempt = 0, MaximumRetries do
		-- Gate on request budget for every attempt, not just retries. Without
		-- this, the first attempt could immediately throttle against an empty
		-- budget and consume a retry allotment for no reason.
		if RequestBudgetType then
			local BudgetResult = Budget.WaitForCapacity(RequestBudgetType)
			if not BudgetResult.Passed then
				return Errors.Fail(Errors.New("BudgetExhausted",
					`Budget exhausted after {Attempt} attempts`,
					{ Retries = Attempt, Key = if LastError then LastError.Key else nil }
				))
			end
		end

		local OperationResult = Operation()
		if OperationResult.Passed then
			return OperationResult
		end

		LastError = (OperationResult :: Types.ErrorResult).Error

		local ErrorKind = LastError.Kind
		if ErrorKind == "ValidationFailed"
			or ErrorKind == "SchemaError"
			or ErrorKind == "StrictModeViolation"
			or ErrorKind == "SessionNotFound"
			or ErrorKind == "SessionClosed"
			or ErrorKind == "TransactionAborted"
		then
			return OperationResult
		end

		if Attempt < MaximumRetries then
			local Delay = ComputeDelay(Attempt)

			if OnRetryCallback then
				OnRetryCallback(Attempt + 1, Delay, LastError)
			end

			task.wait(Delay)
		end
	end

	return Errors.Fail(Errors.New(
		if LastError then LastError.Kind else "Internal",
		`Operation failed after {MaximumRetries} retries: {if LastError then LastError.Message else "unknown"}`,
		{
			Retries = MaximumRetries,
			Cause = if LastError then LastError.Cause else nil,
			Key = if LastError then LastError.Key else nil,
		}
	))
end

function Retry.WrapPcall<Value>(
	Kind : Types.ErrorKind,
	Operation : () -> Value,
	Configuration : RetryConfiguration?
) : Result<Value>
	return Retry.Execute(function() : Result<Value>
		local Success, OperationResult = pcall(Operation)
		if Success then
			return Errors.Passed(OperationResult)
		end
		local ErrorString = tostring(OperationResult)
		local ResolvedKind : Types.ErrorKind = Kind
		if Errors.IsThrottle(ErrorString) then
			ResolvedKind = "Throttled"
		end
		return Errors.Fail(Errors.New(ResolvedKind, ErrorString, { Cause = ErrorString }))
	end, Configuration)
end

return Retry
