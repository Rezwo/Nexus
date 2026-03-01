--!strict

local Types = require(script.Parent.Types)

type NexusError = Types.NexusError
type ErrorKind = Types.ErrorKind
type Result<Value> = Types.Result<Value>

type ErrorResult = Types.ErrorResult

local Errors = {}

function Errors.New(
	Kind : ErrorKind,
	Message : string,
	Extra : {
		Code : number?,
		Key : string?,
		Retries : number?,
		Cause : string?,
	}?
) : NexusError
	return {
		Kind = Kind,
		Message = Message,
		Code = if Extra then Extra.Code else nil,
		Key = if Extra then Extra.Key else nil,
		Retries = if Extra then Extra.Retries else nil,
		Cause = if Extra then Extra.Cause else nil,
	}
end

function Errors.Passed<Value>(WrappedValue : Value) : Result<Value>
	return { Passed = true, Value = WrappedValue }
end

function Errors.Fail(WrappedError : NexusError) : ErrorResult
	return { Passed = false, Error = WrappedError }
end

function Errors.FromPcall<Value>(
	Kind : ErrorKind,
	Success : boolean,
	PcallResult : Value | string,
	Key : string?
) : Result<Value>
	if Success then
		return { Passed = true, Value = PcallResult :: Value }
	end
	return {
		Passed = false,
		Error = Errors.New(Kind, tostring(PcallResult), {
			Cause = tostring(PcallResult),
			Key = Key,
		}),
	}
end

function Errors.IsThrottle(ErrorString : string) : boolean
	local LowerCase = string.lower(ErrorString)
	return string.find(LowerCase, "request was throttled") ~= nil
		or string.find(LowerCase, "too many requests") ~= nil
		or string.find(LowerCase, "budget") ~= nil
end

function Errors.Format(Error : NexusError) : string
	local Parts = { `[Nexus:{Error.Kind}] {Error.Message}` }
	if Error.Key then
		table.insert(Parts,`Key = {Error.Key}`)
	end
	if Error.Retries then
		table.insert(Parts,`Retries = {Error.Retries}`)
	end
	if Error.Cause then
		table.insert(Parts,`Cause = {Error.Cause}`)
	end
	return table.concat(Parts, "\n")
end

return Errors
