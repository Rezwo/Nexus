--!strict

local Errors = require(script.Parent.Errors)
local Types = require(script.Parent.Types)
local Signal = require(script.Parent.Signal)
local Validation = require(script.Parent.Validation)
local Compression = require(script.Parent.Compression)
local Schema = require(script.Parent.Schema)

type Result<Value> = Types.Result<Value>
type NexusError = Types.NexusError
type SessionStatus = Types.SessionStatus
type SessionInfo = Types.SessionInfo
type Envelope<DataType> = Types.Envelope<DataType>
type LockInfo = Types.LockInfo
type SchemaDefinition<DataType> = Types.SchemaDefinition<DataType>
type Signal<T...> = Types.Signal<T...>

local Session = {}
Session.__index = Session

export type SessionObject<DataType> = {

	_Key : string,
	_Data : DataType,
	_Status : SessionStatus,
	_Dirty : boolean,
	_SchemaVersion : number,
	_LockVersion : number,
	_LastSaveTime : number,
	_SaveCount : number,
	_CreatedAt : number,
	_Compress : boolean,
	_CompressionThreshold : number,
	_CompressionLevel : number,
	_Strict : boolean,
	_Schema : SchemaDefinition<DataType>,
	_GetUserIds : ((Key : string) -> { number })?,
	_PendingActions : { (Data : DataType) -> DataType? },
	OnSave : Signal<DataType>,
	OnClose : Signal<>,
	OnError : Signal<NexusError>,

	Read : (self : SessionObject<DataType>) -> Result<DataType>,
	Update : (self : SessionObject<DataType>, Transform : (Data : DataType) -> DataType?) -> Result<DataType>,
	QueueAction : (self : SessionObject<DataType>, Action : (Data : DataType) -> DataType?) -> Result<boolean>,
	BuildEnvelope : (self : SessionObject<DataType>) -> Result<Envelope<DataType>>,
	GetUserIds : (self : SessionObject<DataType>) -> { number },
	MarkSaved : (self : SessionObject<DataType>) -> (),
	GetInfo : (self : SessionObject<DataType>) -> SessionInfo,
	SetStatus : (self : SessionObject<DataType>, Status : SessionStatus) -> (),
	IsDirty : (self : SessionObject<DataType>) -> boolean,
	Close : (self : SessionObject<DataType>) -> (),
}

function Session.New<DataType>(Configuration : {
	Key : string,
	Data : DataType,
	SchemaVersion : number,
	LockVersion : number,
	Compress : boolean,
	CompressionThreshold : number,
	CompressionLevel : number,
	Strict : boolean,
	Schema : SchemaDefinition<DataType>,
	GetUserIds : ((Key : string) -> { number })?,
}) : SessionObject<DataType>
	local self = setmetatable({}, Session)
	self._Key = Configuration.Key
	self._Data = Configuration.Data
	self._Status = "Active" :: SessionStatus
	self._Dirty = false
	self._SchemaVersion = Configuration.SchemaVersion
	self._LockVersion = Configuration.LockVersion
	self._LastSaveTime = os.clock()
	self._SaveCount = 0
	self._CreatedAt = os.clock()
	self._Compress = Configuration.Compress
	self._CompressionThreshold = Configuration.CompressionThreshold
	self._CompressionLevel = Configuration.CompressionLevel
	self._Strict = Configuration.Strict
	self._Schema = Configuration.Schema
	self._GetUserIds = Configuration.GetUserIds
	self._PendingActions = {}
	self.OnSave = Signal.New()
	self.OnClose = Signal.New()
	self.OnError = Signal.New()
	return self :: any
end

function Session.Read<DataType>(self : SessionObject<DataType>) : Result<DataType>
	if self._Status ~= "Active" and self._Status ~= "Saving" then
		return Errors.Fail(Errors.New("SessionClosed",
			`Session '{self._Key}' is {self._Status}, cannot read`,
			{ Key = self._Key }
		))
	end
	return Errors.Passed(Validation.DeepClone(self._Data))
end

function Session.Update<DataType>(
	self : SessionObject<DataType>,
	Transform : (Data : DataType) -> DataType?
) : Result<DataType>
	if self._Status ~= "Active" then
		return Errors.Fail(Errors.New("SessionClosed",
			`Session '{self._Key}' is {self._Status}, cannot update`,
			{ Key = self._Key }
		))
	end

	if self._Strict then
		local PurityResult = Validation.ValidateTransformPurity(Transform, self._Data)
		if not PurityResult.Passed then
			local PurityError = (PurityResult :: Types.ErrorResult).Error
			self.OnError:Fire(PurityError)
			return Errors.Fail(PurityError)
		end
	end

	local ClonedData = Validation.DeepClone(self._Data)
	local TransformSuccess, TransformResult = pcall(Transform, ClonedData)
	if not TransformSuccess then
		local TransformError = Errors.New("Internal",
			`Transform threw: {TransformResult}`,
			{ Key = self._Key, Cause = tostring(TransformResult) }
		)
		self.OnError:Fire(TransformError)
		return Errors.Fail(TransformError)
	end

	if TransformResult == nil then
		return Errors.Passed(self._Data)
	end

	local NewData = TransformResult :: DataType

	if self._Strict then
		local JsonSafetyResult = Validation.ValidateJsonSafe(NewData)
		if not JsonSafetyResult.Passed then
			local JsonError = (JsonSafetyResult :: Types.ErrorResult).Error
			self.OnError:Fire(JsonError)
			return Errors.Fail(JsonError)
		end
	end

	if self._Schema.Validate then
		local IsValid, ValidationReason = self._Schema.Validate(NewData)
		if not IsValid then
			local ValidationError = Errors.New("ValidationFailed",
				`Validation failed after update: {ValidationReason or "unknown"}`,
				{ Key = self._Key }
			)
			self.OnError:Fire(ValidationError)
			return Errors.Fail(ValidationError)
		end
	end

	self._Data = NewData
	self._Dirty = true
	return Errors.Passed(Validation.DeepClone(NewData))
end

function Session.QueueAction<DataType>(
	self : SessionObject<DataType>,
	Action : (Data : DataType) -> DataType?
) : Result<boolean>
	if self._Status ~= "Active" then
		return Errors.Fail(Errors.New("SessionClosed",
			`Session '{self._Key}' is {self._Status}`,
			{ Key = self._Key }
		))
	end

	local UpdateResult = self:Update(Action)
	if not UpdateResult.Passed then
		return Errors.Fail((UpdateResult :: Types.ErrorResult).Error)
	end

	table.insert(self._PendingActions, Action)
	return Errors.Passed(true)
end

function Session.BuildEnvelope<DataType>(self : SessionObject<DataType>) : Result<Envelope<DataType>>
	local PayloadData : DataType | string = self._Data
	local IsCompressed = false

	if self._Compress then
		local EstimatedSize = Compression.EstimateSize(self._Data :: any)
		if EstimatedSize > self._CompressionThreshold then
			local CompressResult = Compression.Compress(self._Data :: any, self._CompressionLevel)
			if CompressResult.Passed then
				PayloadData = (CompressResult :: Types.SuccessResult<string>).Value :: any
				IsCompressed = true
			end
		end
	end

	local EnvelopeData : Envelope<DataType> = {
		_v = self._SchemaVersion,
		_c = if IsCompressed then true else nil,
		_d = PayloadData,
		_lock = nil,
	}

	return Errors.Passed(EnvelopeData)
end

function Session.GetUserIds<DataType>(self : SessionObject<DataType>) : { number }
	if self._GetUserIds then
		return self._GetUserIds(self._Key)
	end
	local ParsedUserId = tonumber(self._Key)
	if ParsedUserId then
		return { ParsedUserId }
	end
	return {}
end

function Session.MarkSaved<DataType>(self : SessionObject<DataType>) : ()
	self._Dirty = false
	self._LastSaveTime = os.clock()
	self._SaveCount += 1
	self._PendingActions = {}
	self.OnSave:Fire(self._Data)
end

function Session.GetInfo<DataType>(self : SessionObject<DataType>) : SessionInfo
	return {
		Key = self._Key,
		Status = self._Status,
		LastSaveTime = self._LastSaveTime,
		LockVersion = self._LockVersion,
		Dirty = self._Dirty,
		SchemaVersion = self._SchemaVersion,
		SaveCount = self._SaveCount,
		SessionAge = os.clock() - self._CreatedAt,
		PendingActionCount = #self._PendingActions,
	}
end

function Session.SetStatus<DataType>(self : SessionObject<DataType>, Status : SessionStatus) : ()
	self._Status = Status
end

function Session.IsDirty<DataType>(self : SessionObject<DataType>) : boolean
	return self._Dirty
end

function Session.Close<DataType>(self : SessionObject<DataType>) : ()
	self._Status = "Closed"
	self.OnClose:Fire()
	self.OnSave:Destroy()
	self.OnClose:Destroy()
	self.OnError:Destroy()
end

function Session.UnpackEnvelope<DataType>(
	RawData : unknown,
	SchemaDefinition : SchemaDefinition<DataType>
) : Result<{ Data : DataType, Version : number }>
	if RawData == nil then
		return Errors.Passed({
			Data = Validation.DeepClone(SchemaDefinition.Default),
			Version = SchemaDefinition.Version,
		})
	end

	if typeof(RawData) ~= "table" then
		local MigrationResult = Schema.Migrate(RawData :: any, 0, SchemaDefinition)
		if not MigrationResult.Passed then
			return Errors.Fail((MigrationResult :: Types.ErrorResult).Error)
		end
		local Migrated = (MigrationResult :: Types.SuccessResult<{ Data : { [string] : unknown }, Version : number }>).Value
		return Errors.Passed({ Data = Migrated.Data :: any, Version = Migrated.Version })
	end

	local EnvelopeTable = (RawData :: any) :: { [string] : unknown }

	if EnvelopeTable._v == nil then
		local MigrationResult = Schema.Migrate(EnvelopeTable :: any, 0, SchemaDefinition)
		if not MigrationResult.Passed then
			return Errors.Fail((MigrationResult :: Types.ErrorResult).Error)
		end
		local Migrated = (MigrationResult :: Types.SuccessResult<{ Data : { [string] : unknown }, Version : number }>).Value
		return Errors.Passed({ Data = Migrated.Data :: any, Version = Migrated.Version })
	end

	local StoredVersion = EnvelopeTable._v :: number
	local IsCompressed = EnvelopeTable._c == true
	local Payload = EnvelopeTable._d

	local UnpackedData : { [string] : unknown }
	if IsCompressed then
		local DecompressResult = Compression.Decompress(Payload :: string)
		if not DecompressResult.Passed then
			return Errors.Fail((DecompressResult :: Types.ErrorResult).Error)
		end
		UnpackedData = (DecompressResult :: Types.SuccessResult<{ [string] : unknown }>).Value
	else
		UnpackedData = Payload :: any
	end

	if StoredVersion ~= SchemaDefinition.Version then
		local MigrationResult = Schema.Migrate(UnpackedData, StoredVersion, SchemaDefinition)
		if not MigrationResult.Passed then
			return Errors.Fail((MigrationResult :: Types.ErrorResult).Error)
		end
		local Migrated = (MigrationResult :: Types.SuccessResult<{ Data : { [string] : unknown }, Version : number }>).Value
		return Errors.Passed({ Data = Migrated.Data :: any, Version = Migrated.Version })
	end

	UnpackedData = Schema.ApplyDefaults(UnpackedData, SchemaDefinition.Default)
	return Errors.Passed({ Data = UnpackedData :: any, Version = StoredVersion })
end

return Session
