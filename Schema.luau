--!strict

local Errors = require(script.Parent.Errors)
local Types = require(script.Parent.Types)
local Validation = require(script.Parent.Validation)

type Result<Value> = Types.Result<Value>
type SchemaDefinition<DataType> = Types.SchemaDefinition<DataType>
type MigrationStep = Types.MigrationStep

local Schema = {}

local function IsArrayLike(Table : { [string] : unknown }) : boolean
	if next(Table) == nil then
		return false
	end
	for k in pairs(Table) do
		if typeof(k) ~= "number" then
			return false
		end
	end
	return (Table :: any)[1] ~= nil
end

function Schema._ReconcileTable(
	Target : { [string] : unknown },
	Source : { [string] : unknown }
) : ()
	if IsArrayLike(Source) or IsArrayLike(Target) then
		return
	end
	for Key, DefaultValue in Source do
		local ExistingValue = Target[Key]
		if ExistingValue == nil then
			Target[Key] = Validation.DeepClone(DefaultValue)
		elseif typeof(ExistingValue) == "table" and typeof(DefaultValue) == "table" then
			Schema._ReconcileTable(
				ExistingValue :: { [string] : unknown },
				DefaultValue :: { [string] : unknown }
			)
		end
	end
end

function Schema.ApplyDefaults<DataType>(
	ExistingData : { [string] : unknown },
	Defaults : DataType
) : { [string] : unknown }
	local DefaultTable = (Defaults :: any) :: { [string] : unknown }
	local MergedData = Validation.DeepClone(ExistingData)
	Schema._ReconcileTable(MergedData, DefaultTable)
	return MergedData
end

function Schema.Migrate<DataType>(
	Data : { [string] : unknown },
	FromVersion : number,
	SchemaDefinition : SchemaDefinition<DataType>
) : Result<{ Data : { [string] : unknown }, Version : number }>
	local TargetVersion = SchemaDefinition.Version
	local Migrations = SchemaDefinition.Migrations

	if FromVersion == TargetVersion then
		local MergedData = Schema.ApplyDefaults(Data, SchemaDefinition.Default)
		return Errors.Passed({ Data = MergedData, Version = TargetVersion })
	end

	if FromVersion > TargetVersion then
		return Errors.Fail(Errors.New(
			"MigrationFailed",
			`Data version ({FromVersion}) is newer than schema version ({TargetVersion}). Downgrade not supported.`
			))
	end

	local CurrentData = Validation.DeepClone(Data)
	local CurrentVersion = FromVersion

	if Migrations then
		local SortedMigrations : { MigrationStep } = table.clone(Migrations)

		table.sort(SortedMigrations, function(StepA : MigrationStep, StepB : MigrationStep)
			return StepA.Version < StepB.Version
		end)

		for _, Step in SortedMigrations do
			if Step.Version > CurrentVersion and Step.Version <= TargetVersion then
				local MigrationSuccess, MigrationResult = pcall(Step.Up, CurrentData)
				if not MigrationSuccess then
					return Errors.Fail(Errors.New(
						"MigrationFailed",
						`Migration to version {Step.Version} failed: {MigrationResult}`,
						{ Cause = tostring(MigrationResult) }
						))
				end
				CurrentData = MigrationResult :: { [string] : unknown }
				CurrentVersion = Step.Version
			end
		end
	end

	CurrentData = Schema.ApplyDefaults(CurrentData, SchemaDefinition.Default)

	if SchemaDefinition.Validate then
		local IsValid, ValidationReason = SchemaDefinition.Validate(CurrentData :: any)
		if not IsValid then
			return Errors.Fail(Errors.New(
				"ValidationFailed",
				`Post-migration validation failed: {ValidationReason or "unknown"}`,
				{ Cause = ValidationReason }
				))
		end
	end

	return Errors.Passed({ Data = CurrentData, Version = TargetVersion })
end

function Schema.Validate<DataType>(
	Data : DataType,
	SchemaDefinition : SchemaDefinition<DataType>
) : Result<boolean>
	if not SchemaDefinition.Validate then
		return Errors.Passed(true)
	end

	local IsValid, ValidationReason = SchemaDefinition.Validate(Data)
	if IsValid then
		return Errors.Passed(true)
	end

	return Errors.Fail(Errors.New("ValidationFailed", ValidationReason or "Schema validation failed"))
end

return Schema