--!strict

local RunService = game:GetService("RunService")

local Errors = require(script.Parent.Errors)
local Types = require(script.Parent.Types)

type Result<Value> = Types.Result<Value>
type NexusError = Types.NexusError

local Validation = {}

function Validation.ShouldDefaultStrict() : boolean
	return RunService:IsStudio()
end

function Validation.IsJsonSafe(
	Value : unknown,
	Path : string?,
	Visited : { [unknown] : boolean }?
) : (boolean, string?)
	local CurrentPath = Path or "root"
	local SeenTables : { [unknown] : boolean } = (Visited or {}) :: any

	if Value == nil then
		return true, nil
	end

	local ValueType = typeof(Value)

	if ValueType == "string" then
		return true, nil
	end

	if ValueType == "number" then
		local NumberValue = Value :: number
		if NumberValue ~= NumberValue then
			return false, `{CurrentPath}: NaN is not JSON-serializable`
		end
		if NumberValue == math.huge or NumberValue == -math.huge then
			return false, `{CurrentPath}: Infinity is not JSON-serializable`
		end
		return true, nil
	end

	if ValueType == "boolean" then
		return true, nil
	end

	if ValueType == "table" then
		local TableValue = Value :: { [unknown] : unknown }

		if SeenTables[TableValue] then
			return false, `{CurrentPath}: Cyclic reference detected`
		end
		SeenTables[TableValue] = true

		for TableKey, TableEntry in TableValue do
			local KeyType = typeof(TableKey)
			if KeyType == "string" then
				local ChildOk, ChildError = Validation.IsJsonSafe(
					TableEntry, `{CurrentPath}.{TableKey}`, SeenTables
				)
				if not ChildOk then
					return false, ChildError
				end
			elseif KeyType == "number" then
				local NumericKey = TableKey :: number
				if NumericKey ~= math.floor(NumericKey) or NumericKey < 1 then
					return false, `{CurrentPath}: Non-positive-integer numeric key [{TableKey}]`
				end
				local ChildOk, ChildError = Validation.IsJsonSafe(
					TableEntry, `{CurrentPath}[{TableKey}]`, SeenTables
				)
				if not ChildOk then
					return false, ChildError
				end
			else
				return false, `{CurrentPath}: Key of type '{KeyType}' is not JSON-serializable`
			end
		end

		SeenTables[TableValue] = nil
		return true, nil
	end

	return false, `{CurrentPath}: Type '{ValueType}' is not JSON-serializable`
end

function Validation.ValidateJsonSafe(Value : unknown) : Result<boolean>
	local IsValid, Reason = Validation.IsJsonSafe(Value)
	if IsValid then
		return Errors.Passed(true)
	end
	return Errors.Fail(Errors.New("StrictModeViolation", Reason :: string))
end

function Validation.DeepEqual(ValueA : unknown, ValueB : unknown) : boolean
	if typeof(ValueA) ~= typeof(ValueB) then
		return false
	end

	if typeof(ValueA) ~= "table" then
		return ValueA == ValueB
	end

	local TableA = ValueA :: { [unknown] : unknown }
	local TableB = ValueB :: { [unknown] : unknown }

	for Key, EntryA in TableA do
		if not Validation.DeepEqual(EntryA, TableB[Key]) then
			return false
		end
	end

	for Key, _ in TableB do
		if TableA[Key] == nil then
			return false
		end
	end

	return true
end

function Validation.DeepClone<DataType>(Value : DataType) : DataType
	if typeof(Value) ~= "table" then
		return Value
	end

	local SourceTable = (Value :: any) :: { [unknown] : unknown }
	local ClonedTable : { [unknown] : unknown } = {}
	for Key, Entry in SourceTable do
		ClonedTable[Key] = Validation.DeepClone(Entry)
	end
	return ClonedTable :: any
end

function Validation.ValidateTransformPurity<DataType>(
	Transform : (Data : DataType) -> DataType?,
	InputData : DataType
) : Result<boolean>
	local ClonedInputA = Validation.DeepClone(InputData)
	local ClonedInputB = Validation.DeepClone(InputData)

	local SuccessA, ResultA = pcall(Transform, ClonedInputA)
	local SuccessB, ResultB = pcall(Transform, ClonedInputB)

	if not SuccessA then
		return Errors.Fail(Errors.New("StrictModeViolation", `Transform threw: {ResultA}`))
	end
	if not SuccessB then
		return Errors.Fail(Errors.New("StrictModeViolation", `Transform threw on second run: {ResultB}`))
	end

	if ResultA == nil and ResultB == nil then
		return Errors.Passed(true)
	end

	if (ResultA == nil) ~= (ResultB == nil) then
		return Errors.Fail(Errors.New(
			"StrictModeViolation",
			"Transform is not deterministic: returned nil on one run but a value on another"
		))
	end

	if not Validation.DeepEqual(ResultA, ResultB) then
		return Errors.Fail(Errors.New(
			"StrictModeViolation",
			"Transform is not deterministic: two identical inputs produced different outputs"
		))
	end

	local JsonSafetyResult = Validation.ValidateJsonSafe(ResultA)
	if not JsonSafetyResult.Passed then
		return JsonSafetyResult
	end

	return Errors.Passed(true)
end

return Validation
