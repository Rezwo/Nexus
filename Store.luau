--!strict

local DataStoreService = game:GetService("DataStoreService")

local Types = require(script.Parent.Types)
local Errors = require(script.Parent.Errors)
local Signal = require(script.Parent.Signal)
local Retry = require(script.Parent.Retry)
local Validation = require(script.Parent.Validation)
local Lock = require(script.Parent.Lock)
local Circuit = require(script.Parent.Circuit)
local SessionModule = require(script.Parent.Session)
local Transaction = require(script.Parent.Transaction)
local Shard = require(script.Parent.Shard)
local Compression = require(script.Parent.Compression)
local Lifecycle = require(script.Parent.Lifecycle)
local Messaging = require(script.Parent.Messaging)
local Semaphore = require(script.Parent.Semaphore)
local Telemetry = require(script.Parent.Telemetry)
local MiddlewareModule = require(script.Parent.Middleware)

type Result<Value> = Types.Result<Value>
type NexusError = Types.NexusError
type StoreConfiguration<DataType> = Types.StoreConfiguration<DataType>
type LockMode = Types.LockMode
type SessionStatus = Types.SessionStatus
type SessionInfo = Types.SessionInfo
type Envelope<DataType> = Types.Envelope<DataType>
type LockInfo = Types.LockInfo
type SchemaDefinition<DataType> = Types.SchemaDefinition<DataType>
type Signal<T...> = Types.Signal<T...>
type Connection = Types.Connection
type VersionInfo = Types.VersionInfo
type VersionEntry<DataType> = Types.VersionEntry<DataType>
type TransactionConfiguration = Types.TransactionConfiguration
type TransactionResult = Types.TransactionResult
type StoreCallbacks<DataType> = Types.StoreCallbacks<DataType>
type AuditEntry = Types.AuditEntry
type PageCursor = Types.PageCursor
type BatchResult<DataType> = Types.BatchResult<DataType>
type DebugContext = Types.DebugContext
type SoftDeleteInfo = Types.SoftDeleteInfo
type TelemetryScope = Telemetry.TelemetryScope
type MiddlewarePipeline = MiddlewareModule.MiddlewarePipeline
type MiddlewareFn = MiddlewareModule.MiddlewareFn
type MiddlewarePhase = Types.MiddlewarePhase

local Store = {}
Store.__index = Store

local WRITE_COOLDOWN = 6
local DEFAULT_IDLE_THRESHOLD = 600
local DEFAULT_AUDIT_LOG_SIZE = 50
local DEFAULT_SOFT_DELETE_RETENTION = 2592000

export type ListVersionsOptions = {
	SortDirection : Enum.SortDirection?,
	MinDate : number?,
	MaxDate : number?,
	PageSize : number?,
	MaxPages : number?,
}

export type CircuitInfo = {
	Name : string,
	State : Circuit.CircuitState,
	FailureCount : number,
	SuccessCount : number,
	LastFailureTime : number,
	TimeSinceStateChange : number,
}

export type StoreObject<DataType> = {
	_Configuration : StoreConfiguration<DataType>,
	_DataStore : DataStore,
	_LockManager : Lock.LockManager?,
	_Sessions : { [string] : SessionModule.SessionObject<DataType> },
	_AutoSaveThread : thread?,
	_IdleThread : thread?,
	_Callbacks : StoreCallbacks<DataType>,
	_LastWriteTime : { [string] : number },
	_ShardCounts : { [string] : number },
	_ShuttingDown : boolean,
	_AuditLogs : { [string] : { AuditEntry } },
	_SoftDeleted : { [string] : { DeletedAt : number, EnvelopeSnapshot : unknown } },
	_DebounceTimers : { [string] : thread },
	_LastActivity : { [string] : number },
	_Telemetry : TelemetryScope,
	_Middleware : MiddlewarePipeline,
	_Debug : boolean,

	OnLoad : Signal<string, DataType>,
	OnSave : Signal<string, DataType>,
	OnRelease : Signal<string>,
	OnError : Signal<string, NexusError>,
	OnDirty : Signal<string>,
	OnIdle : Signal<string, number>,

	Load : (self : StoreObject<DataType>, Key : string) -> Result<DataType>,
	Peek : (self : StoreObject<DataType>, Key : string) -> Result<DataType>,
	LoadMany : (self : StoreObject<DataType>, Keys : { string }) -> BatchResult<DataType>,
	Read : (self : StoreObject<DataType>, Key : string) -> Result<DataType>,
	Update : (self : StoreObject<DataType>, Key : string, Transform : (Data : DataType) -> DataType?) -> Result<DataType>,
	Save : (self : StoreObject<DataType>, Key : string) -> Result<boolean>,
	Close : (self : StoreObject<DataType>, Key : string) -> Result<boolean>,
	CloseAll : (self : StoreObject<DataType>) -> (),
	SaveAll : (self : StoreObject<DataType>) -> (),
	SaveOptimistic : (self : StoreObject<DataType>, Key : string) -> Result<boolean>,
	Transact : (self : StoreObject<DataType>, Participants : { { Key : string, Transform : (Data : DataType) -> DataType? } }, Configuration : TransactionConfiguration?) -> TransactionResult,
	ListVersions : (self : StoreObject<DataType>, Key : string, Options : ListVersionsOptions?) -> Result<{ VersionInfo }>,
	GetVersion : (self : StoreObject<DataType>, Key : string, VersionId : string) -> Result<VersionEntry<DataType>>,
	Rollback : (self : StoreObject<DataType>, Key : string, VersionId : string) -> Result<DataType>,
	ListKeys : (self : StoreObject<DataType>, Prefix : string?, PageSize : number?, MaxPages : number?) -> Result<{ string }>,
	ListKeysPage : (self : StoreObject<DataType>, Prefix : string?, PageSize : number?) -> Result<PageCursor>,
	NextPage : (self : StoreObject<DataType>, Cursor : PageCursor) -> Result<{ string }>,
	SoftDelete : (self : StoreObject<DataType>, Key : string) -> Result<boolean>,
	Recover : (self : StoreObject<DataType>, Key : string) -> Result<DataType>,
	IsSoftDeleted : (self : StoreObject<DataType>, Key : string) -> boolean,
	ListSoftDeleted : (self : StoreObject<DataType>) -> { SoftDeleteInfo },
	PurgeSoftDeleted : (self : StoreObject<DataType>, Key : string) -> Result<boolean>,
	GetAuditLog : (self : StoreObject<DataType>, Key : string) -> { AuditEntry },
	GetTelemetry : (self : StoreObject<DataType>) -> TelemetryScope,
	Use : (self : StoreObject<DataType>, Phase : MiddlewarePhase, Handler : MiddlewareFn) -> (),
	SetCallbacks : (self : StoreObject<DataType>, Callbacks : StoreCallbacks<DataType>) -> (),
	GetSessionInfo : (self : StoreObject<DataType>, Key : string) -> SessionInfo?,
	GetCircuitInfo : (self : StoreObject<DataType>) -> CircuitInfo?,
	GetActiveKeys : (self : StoreObject<DataType>) -> { string },
	IsLoaded : (self : StoreObject<DataType>, Key : string) -> boolean,
	Remove : (self : StoreObject<DataType>, Key : string) -> Result<boolean>,
	Destroy : (self : StoreObject<DataType>) -> (),

	_PerformCloseSave : (self : StoreObject<DataType>, Key : string, TargetSession : SessionModule.SessionObject<DataType>) -> (),
	_StartAutoSave : (self : StoreObject<DataType>) -> (),
	_StartIdleTracker : (self : StoreObject<DataType>) -> (),
	_HandleRemoteInvalidation : (self : StoreObject<DataType>, Key : string, NewHolder : string) -> (),
	_WaitForWriteCooldown : (self : StoreObject<DataType>, Key : string) -> (),
	_WriteEnvelope : (self : StoreObject<DataType>, Key : string, WriteEnvelope : Envelope<DataType>, UserIds : { number }?) -> Result<boolean>,
	_WriteEnvelopeInner : (self : StoreObject<DataType>, Key : string, WriteEnvelope : Envelope<DataType>, UserIds : { number }?) -> Result<boolean>,
	_RecordAudit : (self : StoreObject<DataType>, Key : string, Operation : string, Before : unknown?, After : unknown?) -> (),
	_DebugLog : (self : StoreObject<DataType>, Operation : string, Key : string?, Elapsed : number?) -> (),
	_TouchActivity : (self : StoreObject<DataType>, Key : string) -> (),
}

local GlobalDebugMode = false
local GlobalDebugHandler : ((DebugContext) -> ())? = nil

function Store.SetGlobalDebugMode(Enabled : boolean) : ()
	GlobalDebugMode = Enabled
end

function Store.SetGlobalDebugHandler(Handler : ((DebugContext) -> ())?) : ()
	GlobalDebugHandler = Handler
end


function Store.New<DataType>(Configuration : StoreConfiguration<DataType>) : StoreObject<DataType>
	local self = setmetatable({}, Store) :: any

	assert(Configuration.Name and #Configuration.Name > 0, "Store name is required")
	assert(Configuration.Schema, "Schema definition is required")
	assert(Configuration.Schema.Version >= 1, "Schema version must be >= 1")
	assert(Configuration.Schema.Default ~= nil, "Schema default is required")

	local ResolvedConfiguration : StoreConfiguration<DataType> = {
		Name = Configuration.Name,
		Scope = Configuration.Scope or "global",
		Schema = Configuration.Schema,
		LockMode = (Configuration.LockMode or "Session") :: LockMode,
		AutoSaveInterval = Configuration.AutoSaveInterval or 300,
		Compress = Configuration.Compress or false,
		CompressionThreshold = Configuration.CompressionThreshold or 4096,
		CompressionLevel = Configuration.CompressionLevel or 3,
		MaxRetries = Configuration.MaxRetries or 5,
		Strict = if Configuration.Strict ~= nil
			then Configuration.Strict
			else Validation.ShouldDefaultStrict(),
		GetUserIds = Configuration.GetUserIds,
		ShardThreshold = if Configuration.ShardThreshold ~= nil
			then Configuration.ShardThreshold
			else 3_500_000,
		ValidateOnLoad = Configuration.ValidateOnLoad or false,
		DebounceInterval = Configuration.DebounceInterval,
		SoftDeleteRetention = Configuration.SoftDeleteRetention or DEFAULT_SOFT_DELETE_RETENTION,
		AuditLog = Configuration.AuditLog or false,
		AuditLogSize = Configuration.AuditLogSize or DEFAULT_AUDIT_LOG_SIZE,
		Debug = Configuration.Debug or false,
	}

	self._Configuration = ResolvedConfiguration
	self._Debug = ResolvedConfiguration.Debug or false

	local DataStoreOptions = Instance.new("DataStoreOptions")
	DataStoreOptions.AllScopes = false
	self._DataStore = DataStoreService:GetDataStore(
		Configuration.Name, ResolvedConfiguration.Scope, DataStoreOptions
	)

	if ResolvedConfiguration.LockMode == "Session" then
		self._LockManager = Lock.New(Configuration.Name)
	end

	self._Sessions = {}
	self._AutoSaveThread = nil
	self._IdleThread = nil
	self._Callbacks = {}
	self._LastWriteTime = {}
	self._ShardCounts = {}
	self._ShuttingDown = false
	self._AuditLogs = {}
	self._SoftDeleted = {}
	self._DebounceTimers = {}
	self._LastActivity = {}
	self._Telemetry = Telemetry.CreateScope(Configuration.Name)
	self._Middleware = MiddlewareModule.New()

	self.OnLoad = Signal.New()
	self.OnSave = Signal.New()
	self.OnRelease = Signal.New()
	self.OnError = Signal.New()
	self.OnDirty = Signal.New()
	self.OnIdle = Signal.New()

	self:_StartAutoSave()
	self:_StartIdleTracker()

	Lifecycle.Register(self)

	if self._LockManager then
		Messaging.Subscribe(Configuration.Name, function(Key : string, NewHolder : string)
			self:_HandleRemoteInvalidation(Key, NewHolder)
		end)
	end

	return self
end

function Store._DebugLog<DataType>(self : StoreObject<DataType>, Operation : string, Key : string?, Elapsed : number?) : ()
	if not self._Debug and not GlobalDebugMode then return end
	local Context : DebugContext = {
		Store = self._Configuration.Name,
		Key = Key,
		Operation = Operation,
		Elapsed = Elapsed,
	}
	if GlobalDebugHandler then
		GlobalDebugHandler(Context)
	else
		local Msg = `[Nexus:{self._Configuration.Name}] {Operation}`
		if Key then Msg ..= ` key='{Key}'` end
		if Elapsed then Msg ..= ` ({string.format("%.1f", Elapsed * 1000)}ms)` end
		print(Msg)
	end
end

function Store._RecordAudit<DataType>(self : StoreObject<DataType>, Key : string, Operation : string, Before : unknown?, After : unknown?) : ()
	if not self._Configuration.AuditLog then return end
	local Log = self._AuditLogs[Key]
	if not Log then
		Log = {}
		self._AuditLogs[Key] = Log
	end
	local MaxSize = self._Configuration.AuditLogSize or DEFAULT_AUDIT_LOG_SIZE
	if #Log >= MaxSize then
		table.remove(Log, 1)
	end
	table.insert(Log, {
		Timestamp = os.time(),
		Operation = Operation,
		Before = Before,
		After = After,
		Source = game.JobId ~= "" and game.JobId or "Studio",
		SessionId = self._Configuration.Name .. ":" .. Key,
	} :: AuditEntry)
end

function Store._TouchActivity<DataType>(self : StoreObject<DataType>, Key : string) : ()
	self._LastActivity[Key] = os.clock()
end


function Store.Load<DataType>(self : StoreObject<DataType>, Key : string) : Result<DataType>
	local StartTime = os.clock()
	self:_DebugLog("Load.Start", Key)

	if self._Sessions[Key] then
		local ExistingSession = self._Sessions[Key]
		if ExistingSession._Status == "Active" or ExistingSession._Status == "Saving" then
			local SessionError = Errors.New("SessionLocked",
				`Key '{Key}' already has an active session on this server`, { Key = Key })
			self._Telemetry:Record("Store.Load", os.clock() - StartTime, false, SessionError)
			return Errors.Fail(SessionError)
		end
		if ExistingSession._Status ~= "Closed" then
			ExistingSession:Close()
		end
		self._Sessions[Key] = nil
	end

	local FetchResult = Retry.WrapPcall("DataStoreError", function()
		return self._DataStore:GetAsync(Key)
	end, { MaxRetries = self._Configuration.MaxRetries, BudgetType = "GetAsync" } :: any)

	if not FetchResult.Passed then
		local FetchError = (FetchResult :: Types.ErrorResult).Error
		FetchError.Key = Key
		self.OnError:Fire(Key, FetchError)
		self._Telemetry:Record("Store.Load", os.clock() - StartTime, false, FetchError)
		return Errors.Fail(FetchError)
	end

	local RawData = (FetchResult :: Types.SuccessResult<unknown>).Value

	if RawData ~= nil and typeof(RawData) == "table" then
		local ManifestTable = RawData :: any
		if ManifestTable._deleted == true then
			local DeletedError = Errors.New("KeyNotFound",
				`Key '{Key}' has been soft-deleted`, { Key = Key })
			self._Telemetry:Record("Store.Load", os.clock() - StartTime, false, DeletedError)
			return Errors.Fail(DeletedError)
		end
		local ShardCount = ManifestTable._s
		if typeof(ShardCount) == "number" and ShardCount > 0 then
			local ShardResult = Shard.ReadShards(self._DataStore, Key, ShardCount, self._Configuration.MaxRetries)
			if not ShardResult.Passed then
				local ShardError = (ShardResult :: Types.ErrorResult).Error
				ShardError.Key = Key
				self.OnError:Fire(Key, ShardError)
				self._Telemetry:Record("Store.Load", os.clock() - StartTime, false, ShardError)
				return Errors.Fail(ShardError)
			end
			ManifestTable._d = (ShardResult :: Types.SuccessResult<string>).Value
			ManifestTable._c = true
			ManifestTable._s = nil
			self._ShardCounts[Key] = ShardCount
		end
	end

	local EnvelopeLock : LockInfo? = nil
	if RawData ~= nil and typeof(RawData) == "table" then
		local EnvelopeTable = RawData :: any
		if EnvelopeTable._lock ~= nil then
			EnvelopeLock = EnvelopeTable._lock :: LockInfo
		end
	end

	local AcquiredLockVersion = 0
	if self._LockManager then
		local LockResult = self._LockManager:Acquire(Key, EnvelopeLock)
		if not LockResult.Passed then
			local LockError = (LockResult :: Types.ErrorResult).Error
			self.OnError:Fire(Key, LockError)
			if self._Callbacks.OnError then self._Callbacks.OnError(Key, LockError) end
			self._Telemetry:Record("Store.Load", os.clock() - StartTime, false, LockError)
			return Errors.Fail(LockError)
		end
		AcquiredLockVersion = (LockResult :: Types.SuccessResult<Lock.Lease>).Value.LeaseVersion
	end

	local UnpackResult = SessionModule.UnpackEnvelope(RawData, self._Configuration.Schema)
	if not UnpackResult.Passed then
		if self._LockManager then self._LockManager:Release(Key) end
		local UnpackError = (UnpackResult :: Types.ErrorResult).Error
		UnpackError.Key = Key
		self.OnError:Fire(Key, UnpackError)
		self._Telemetry:Record("Store.Load", os.clock() - StartTime, false, UnpackError)
		return Errors.Fail(UnpackError)
	end

	local UnpackedData = (UnpackResult :: Types.SuccessResult<{ Data : DataType, Version : number }>).Value
	local LoadedData = UnpackedData.Data

	if self._Configuration.ValidateOnLoad and self._Configuration.Schema.Validate then
		local IsValid, Reason = self._Configuration.Schema.Validate(LoadedData)
		if not IsValid then
			if self._LockManager then self._LockManager:Release(Key) end
			local ValError = Errors.New("ValidationFailed", `Validation on load failed: {Reason or "unknown"}`, { Key = Key })
			self._Telemetry:Record("Store.Load", os.clock() - StartTime, false, ValError)
			return Errors.Fail(ValError)
		end
	end

	if self._Callbacks.AfterLoad then
		local Transformed = self._Callbacks.AfterLoad(Key, LoadedData)
		if Transformed ~= nil then
			LoadedData = Transformed
		end
	end

	if self._Middleware:HasMiddleware("load") then
		local MiddlewareResult = self._Middleware:Run("load", Key, LoadedData)
		if MiddlewareResult ~= nil then
			LoadedData = MiddlewareResult :: DataType
		end
	end

	local NewSession = SessionModule.New({
		Key = Key,
		Data = LoadedData,
		SchemaVersion = UnpackedData.Version,
		LockVersion = AcquiredLockVersion,
		Compress = self._Configuration.Compress or false,
		CompressionThreshold = self._Configuration.CompressionThreshold or 4096,
		CompressionLevel = self._Configuration.CompressionLevel or 3,
		Strict = self._Configuration.Strict or false,
		Schema = self._Configuration.Schema,
		GetUserIds = self._Configuration.GetUserIds,
	})

	self._Sessions[Key] = NewSession
	self:_TouchActivity(Key)
	self:_RecordAudit(Key, "Load", nil, LoadedData)

	self.OnLoad:Fire(Key, LoadedData)
	if self._Callbacks.OnLoad then self._Callbacks.OnLoad(Key, LoadedData) end
	if self._Callbacks.OnMigration and RawData ~= nil then
		local RawEnvelope = RawData :: any
		local OldVersion = if typeof(RawEnvelope) == "table" and RawEnvelope._v
			then RawEnvelope._v :: number else 0
		if OldVersion ~= UnpackedData.Version then
			self._Callbacks.OnMigration(Key, OldVersion, UnpackedData.Version)
		end
	end

	self._Telemetry:Record("Store.Load", os.clock() - StartTime, true)
	self:_DebugLog("Load.Done", Key, os.clock() - StartTime)
	return Errors.Passed(LoadedData)
end

function Store.Peek<DataType>(self : StoreObject<DataType>, Key : string) : Result<DataType>
	local StartTime = os.clock()
	self:_DebugLog("Peek.Start", Key)

	local FetchResult = Retry.WrapPcall("DataStoreError", function()
		return self._DataStore:GetAsync(Key)
	end, { MaxRetries = self._Configuration.MaxRetries, BudgetType = "GetAsync" } :: any)

	if not FetchResult.Passed then
		self._Telemetry:Record("Store.Peek", os.clock() - StartTime, false, (FetchResult :: Types.ErrorResult).Error)
		return Errors.Fail((FetchResult :: Types.ErrorResult).Error)
	end

	local RawData = (FetchResult :: Types.SuccessResult<unknown>).Value

	if RawData ~= nil and typeof(RawData) == "table" then
		local ManifestTable = RawData :: any
		local ShardCount = ManifestTable._s
		if typeof(ShardCount) == "number" and ShardCount > 0 then
			local ShardResult = Shard.ReadShards(self._DataStore, Key, ShardCount, self._Configuration.MaxRetries)
			if not ShardResult.Passed then
				self._Telemetry:Record("Store.Peek", os.clock() - StartTime, false, (ShardResult :: Types.ErrorResult).Error)
				return Errors.Fail((ShardResult :: Types.ErrorResult).Error)
			end
			ManifestTable._d = (ShardResult :: Types.SuccessResult<string>).Value
			ManifestTable._c = true
			ManifestTable._s = nil
		end
	end

	local UnpackResult = SessionModule.UnpackEnvelope(RawData, self._Configuration.Schema)
	if not UnpackResult.Passed then
		self._Telemetry:Record("Store.Peek", os.clock() - StartTime, false, (UnpackResult :: Types.ErrorResult).Error)
		return Errors.Fail((UnpackResult :: Types.ErrorResult).Error)
	end

	local Data = (UnpackResult :: Types.SuccessResult<{ Data : DataType, Version : number }>).Value.Data
	self._Telemetry:Record("Store.Peek", os.clock() - StartTime, true)
	self:_DebugLog("Peek.Done", Key, os.clock() - StartTime)
	return Errors.Passed(Data)
end

function Store.LoadMany<DataType>(self : StoreObject<DataType>, Keys : { string }) : BatchResult<DataType>
	local Succeeded : { [string] : DataType } = {}
	local Failed : { [string] : NexusError } = {}
	local Remaining = #Keys
	local CompletionSignal = Instance.new("BindableEvent")

	for _, Key in Keys do
		task.spawn(function()
			local LoadResult = self:Load(Key)
			if LoadResult.Passed then
				Succeeded[Key] = (LoadResult :: Types.SuccessResult<DataType>).Value
			else
				Failed[Key] = (LoadResult :: Types.ErrorResult).Error
			end
			Remaining -= 1
			if Remaining == 0 then CompletionSignal:Fire() end
		end)
	end

	if Remaining > 0 then
		CompletionSignal.Event:Wait()
	end
	CompletionSignal:Destroy()

	return { Succeeded = Succeeded, Failed = Failed }
end


function Store.Read<DataType>(self : StoreObject<DataType>, Key : string) : Result<DataType>
	local TargetSession = self._Sessions[Key]
	if not TargetSession then
		return Errors.Fail(Errors.New("SessionNotFound", `No active session for key '{Key}'`, { Key = Key }))
	end
	self:_TouchActivity(Key)
	return TargetSession:Read()
end

function Store.Update<DataType>(
	self : StoreObject<DataType>,
	Key : string,
	Transform : (Data : DataType) -> DataType?
) : Result<DataType>
	local TargetSession = self._Sessions[Key]
	if not TargetSession then
		return Errors.Fail(Errors.New("SessionNotFound", `No active session for key '{Key}'`, { Key = Key }))
	end
	local WasDirty = TargetSession:IsDirty()
	local Before = if self._Configuration.AuditLog then Validation.DeepClone(TargetSession._Data) else nil
	local UpdateResult = TargetSession:Update(Transform)
	if UpdateResult.Passed then
		self:_TouchActivity(Key)
		if not WasDirty and TargetSession:IsDirty() then
			self.OnDirty:Fire(Key)
		end
		if self._Configuration.AuditLog then
			self:_RecordAudit(Key, "Update", Before, TargetSession._Data)
		end
	end
	return UpdateResult
end

function Store.Save<DataType>(self : StoreObject<DataType>, Key : string) : Result<boolean>
	local TargetSession = self._Sessions[Key]
	if not TargetSession then
		return Errors.Fail(Errors.New("SessionNotFound", `No active session for key '{Key}'`, { Key = Key }))
	end

	if not TargetSession:IsDirty() then
		return Errors.Passed(true)
	end

	TargetSession:SetStatus("Saving")

	if not self._ShuttingDown then
		self:_WaitForWriteCooldown(Key)
	end

	local EnvelopeResult = TargetSession:BuildEnvelope()
	if not EnvelopeResult.Passed then
		TargetSession:SetStatus("Active")
		return Errors.Fail((EnvelopeResult :: Types.ErrorResult).Error)
	end

	local SaveEnvelope = (EnvelopeResult :: Types.SuccessResult<Envelope<DataType>>).Value
	local UserIds = TargetSession:GetUserIds()

	if self._Callbacks.BeforeSave then
		local TransformedData = self._Callbacks.BeforeSave(Key, TargetSession._Data)
		if TransformedData == nil then
			TargetSession:SetStatus("Active")
			return Errors.Passed(true)
		end
	end

	if self._Middleware:HasMiddleware("save") then
		self._Middleware:Run("save", Key, TargetSession._Data)
	end

	if self._LockManager then
		local CurrentLockInfo = self._LockManager:ToLockInfo(Key)
		if CurrentLockInfo then
			SaveEnvelope._lock = CurrentLockInfo
		end
	end

	local WriteResult = self:_WriteEnvelope(Key, SaveEnvelope, UserIds)
	self._LastWriteTime[Key] = os.clock()

	if not WriteResult.Passed then
		TargetSession:SetStatus("Active")
		local SaveError = (WriteResult :: Types.ErrorResult).Error
		SaveError.Key = Key
		self.OnError:Fire(Key, SaveError)
		return Errors.Fail(SaveError)
	end

	TargetSession:SetStatus("Active")
	TargetSession:MarkSaved()
	self:_RecordAudit(Key, "Save", nil, TargetSession._Data)

	self.OnSave:Fire(Key, TargetSession._Data)
	if self._Callbacks.OnSave then self._Callbacks.OnSave(Key, TargetSession._Data) end

	return Errors.Passed(true)
end

function Store.Close<DataType>(self : StoreObject<DataType>, Key : string) : Result<boolean>
	local TargetSession = self._Sessions[Key]
	if not TargetSession then
		return Errors.Fail(Errors.New("SessionNotFound", `No active session for key '{Key}'`, { Key = Key }))
	end

	TargetSession:SetStatus("Closing")

	local DebounceThread = self._DebounceTimers[Key]
	if DebounceThread then
		task.cancel(DebounceThread)
		self._DebounceTimers[Key] = nil
	end

	local NeedsSave = TargetSession:IsDirty() or self._LockManager ~= nil
	if NeedsSave then
		self:_PerformCloseSave(Key, TargetSession)
	end

	if self._LockManager then
		self._LockManager:Release(Key)
	end

	self:_RecordAudit(Key, "Close", nil, nil)
	TargetSession:Close()
	self._Sessions[Key] = nil
	self._LastWriteTime[Key] = nil
	self._ShardCounts[Key] = nil
	self._LastActivity[Key] = nil

	self.OnRelease:Fire(Key)
	if self._Callbacks.OnRelease then self._Callbacks.OnRelease(Key) end

	return Errors.Passed(true)
end

function Store._PerformCloseSave<DataType>(
	self : StoreObject<DataType>,
	Key : string,
	TargetSession : SessionModule.SessionObject<DataType>
) : ()
	local EnvelopeResult = TargetSession:BuildEnvelope()
	if not EnvelopeResult.Passed then
		warn(`[Nexus] Close-save envelope build failed for '{Key}'`)
		return
	end

	local CloseEnvelope = (EnvelopeResult :: Types.SuccessResult<Envelope<DataType>>).Value
	local UserIds = TargetSession:GetUserIds()

	if self._LockManager then
		CloseEnvelope._lock = Lock.ToReleasedLockInfo()
	end

	if not self._ShuttingDown then
		self:_WaitForWriteCooldown(Key)
	end

	local WriteResult = self:_WriteEnvelope(Key, CloseEnvelope, UserIds)
	self._LastWriteTime[Key] = os.clock()

	if not WriteResult.Passed then
		warn(`[Nexus] Close-save failed for '{Key}': {(WriteResult :: Types.ErrorResult).Error.Message}`)
	else
		TargetSession:MarkSaved()
	end
end

function Store.CloseAll<DataType>(self : StoreObject<DataType>) : ()
	if self._AutoSaveThread then
		task.cancel(self._AutoSaveThread)
		self._AutoSaveThread = nil
	end
	if self._IdleThread then
		task.cancel(self._IdleThread)
		self._IdleThread = nil
	end

	local SessionKeys : { string } = {}
	for Key, _ in self._Sessions do
		table.insert(SessionKeys, Key)
	end
	if #SessionKeys == 0 then return end

	local RemainingCount = #SessionKeys
	local CompletionSignal = Instance.new("BindableEvent")

	for _, Key in SessionKeys do
		task.spawn(function()
			local Success, ErrorMessage = pcall(function()
				self:Close(Key)
				return nil :: any
			end)
			if not Success then
				warn(`[Nexus] CloseAll: error closing '{Key}': {ErrorMessage}`)
			end
			RemainingCount -= 1
			if RemainingCount == 0 then CompletionSignal:Fire() end
		end)
	end

	if RemainingCount > 0 then CompletionSignal.Event:Wait() end
	CompletionSignal:Destroy()
end

function Store._StartAutoSave<DataType>(self : StoreObject<DataType>) : ()
	self._AutoSaveThread = task.spawn(function()
		while true do
			task.wait(self._Configuration.AutoSaveInterval or 300)
			self:SaveAll()
		end
	end)
end

function Store._StartIdleTracker<DataType>(self : StoreObject<DataType>) : ()
	self._IdleThread = task.spawn(function()
		while true do
			task.wait(60)
			local CurrentTime = os.clock()
			for Key, LastActive in self._LastActivity do
				local IdleTime = CurrentTime - LastActive
				if IdleTime >= DEFAULT_IDLE_THRESHOLD then
					self.OnIdle:Fire(Key, IdleTime)
				end
			end
		end
	end)
end

function Store.SaveAll<DataType>(self : StoreObject<DataType>) : ()
	for Key, TargetSession in self._Sessions do
		if TargetSession:IsDirty() and TargetSession._Status == "Active" then
			local SaveResult = self:Save(Key)
			if not SaveResult.Passed then
				warn(`[Nexus] Auto-save failed for '{Key}': {(SaveResult :: Types.ErrorResult).Error.Message}`)
			end
			if not self._ShuttingDown then task.wait(0.5) end
		end
	end
end


function Store._HandleRemoteInvalidation<DataType>(self : StoreObject<DataType>, Key : string, NewHolder : string) : ()
	local TargetSession = self._Sessions[Key]
	if not TargetSession then return end
	if TargetSession._Status == "Closed" or TargetSession._Status == "Closing" then return end
	warn(`[Nexus] Session '{Key}' invalidated by remote server {NewHolder}`)
	if self._LockManager then self._LockManager:Invalidate(Key) end
	TargetSession:SetStatus("Error")
	local InvalidationError = Errors.New("SessionLocked",
		`Session for '{Key}' was invalidated: lease stolen by server {NewHolder}`, { Key = Key })
	TargetSession.OnError:Fire(InvalidationError)
	self.OnError:Fire(Key, InvalidationError)
	if self._Callbacks.OnError then self._Callbacks.OnError(Key, InvalidationError) end
end

function Store._WaitForWriteCooldown<DataType>(self : StoreObject<DataType>, Key : string) : ()
	local LastWriteTimestamp = self._LastWriteTime[Key]
	if LastWriteTimestamp then
		local ElapsedTime = os.clock() - LastWriteTimestamp
		if ElapsedTime < WRITE_COOLDOWN then
			task.wait(WRITE_COOLDOWN - ElapsedTime)
		end
	end
end

function Store._WriteEnvelope<DataType>(
	self : StoreObject<DataType>,
	Key : string,
	WriteEnvelope : Envelope<DataType>,
	UserIds : { number }?
) : Result<boolean>
	local StartTime = os.clock()
	Semaphore.Global:Acquire()
	local WriteResult = self:_WriteEnvelopeInner(Key, WriteEnvelope, UserIds)
	Semaphore.Global:Release()
	self._Telemetry:Record("Store.Write", os.clock() - StartTime, WriteResult.Passed)
	self:_DebugLog("Write", Key, os.clock() - StartTime)
	return WriteResult
end

function Store._WriteEnvelopeInner<DataType>(
	self : StoreObject<DataType>,
	Key : string,
	WriteEnvelope : Envelope<DataType>,
	UserIds : { number }?
) : Result<boolean>
	local ShardThreshold = self._Configuration.ShardThreshold or 3_500_000
	local RetryCount = if self._ShuttingDown then 2 else self._Configuration.MaxRetries
	local PayloadData = WriteEnvelope._d
	local OldShardCount = self._ShardCounts[Key] or 0

	local NeedsSharding = typeof(PayloadData) == "string"
		and ShardThreshold > 0
		and #(PayloadData :: string) > ShardThreshold

	if NeedsSharding then
		local CompressedPayload = PayloadData :: string
		local Chunks = Shard.Split(CompressedPayload, ShardThreshold)
		local NewShardCount = #Chunks

		local ShardWriteResult = Shard.WriteShards(self._DataStore, Key, Chunks, RetryCount)
		if not ShardWriteResult.Passed then
			return Errors.Fail((ShardWriteResult :: Types.ErrorResult).Error)
		end

		local Manifest : Envelope<DataType> = {
			_v = WriteEnvelope._v,
			_c = WriteEnvelope._c,
			_d = nil :: any,
			_lock = WriteEnvelope._lock,
			_s = NewShardCount,
		}

		local ManifestResult = Retry.WrapPcall("DataStoreError", function()
			self._DataStore:UpdateAsync(Key, function()
				local Metadata = { SchemaVersion = tostring(Manifest._v) }
				return Manifest :: any, UserIds or {}, Metadata
			end)
			return true
		end, { MaxRetries = RetryCount, BudgetType = "UpdateAsync" } :: any)

		if not ManifestResult.Passed then
			return Errors.Fail((ManifestResult :: Types.ErrorResult).Error)
		end

		if OldShardCount > NewShardCount then
			Shard.CleanupOrphanedShards(self._DataStore, Key, OldShardCount, NewShardCount)
		end

		self._ShardCounts[Key] = NewShardCount
		return Errors.Passed(true)
	end

	local SaveResult = Retry.WrapPcall("DataStoreError", function()
		self._DataStore:UpdateAsync(Key, function()
			local Metadata = { SchemaVersion = tostring(WriteEnvelope._v) }
			return WriteEnvelope :: any, UserIds or {}, Metadata
		end)
		return true
	end, { MaxRetries = RetryCount, BudgetType = "UpdateAsync" } :: any)

	if not SaveResult.Passed then
		return Errors.Fail((SaveResult :: Types.ErrorResult).Error)
	end

	if OldShardCount > 0 then
		Shard.DeleteAllShards(self._DataStore, Key, OldShardCount)
		self._ShardCounts[Key] = 0
	end

	return Errors.Passed(true)
end

function Store.SaveOptimistic<DataType>(self : StoreObject<DataType>, Key : string) : Result<boolean>
	local TargetSession = self._Sessions[Key]
	if not TargetSession then
		return Errors.Fail(Errors.New("SessionNotFound", `No session for key '{Key}'`, { Key = Key }))
	end

	local PendingActions = table.clone(TargetSession._PendingActions)
	if #PendingActions == 0 then return Errors.Passed(true) end
	local ReplayedActionCount = #PendingActions

	if (self._ShardCounts[Key] or 0) > 0 then
		return Errors.Fail(Errors.New("Internal",
			`Key '{Key}' is sharded. Optimistic saves are incompatible with sharded data.`, { Key = Key }))
	end

	TargetSession:SetStatus("Saving")

	if not self._ShuttingDown then
		self:_WaitForWriteCooldown(Key)
	end

	local SchemaDefinition = self._Configuration.Schema
	local CurrentLockManager = self._LockManager
	local ShouldCompress = self._Configuration.Compress or false
	local CompressionThreshold = self._Configuration.CompressionThreshold or 4096
	local CompressionLevel = self._Configuration.CompressionLevel or 3
	local DidCommit = false

	local SaveResult = Retry.WrapPcall("DataStoreError", function()
		self._DataStore:UpdateAsync(Key, function(CurrentRemoteData : unknown, _KeyInfo : DataStoreKeyInfo)
			DidCommit = false

			if typeof(CurrentRemoteData) == "table" then
				local RemoteTable = CurrentRemoteData :: any
				if typeof(RemoteTable._s) == "number" and RemoteTable._s > 0 then return nil end
			end

			local UnpackResult = SessionModule.UnpackEnvelope(CurrentRemoteData, SchemaDefinition)
			if not UnpackResult.Passed then return nil end

			local UnpackedData = (UnpackResult :: Types.SuccessResult<{ Data : DataType, Version : number }>).Value
			local ReplayedData : DataType = UnpackedData.Data

			for _, Action in PendingActions do
				local ActionResult = Action(ReplayedData)
				if ActionResult ~= nil then ReplayedData = ActionResult end
			end

			if SchemaDefinition.Validate then
				local IsValid, ValidationReason = SchemaDefinition.Validate(ReplayedData)
				if not IsValid then
					warn(`[Nexus] SaveOptimistic: validation failed for '{Key}': {ValidationReason or "unknown"}`)
					return nil
				end
			end

			local PayloadData : DataType | string = ReplayedData
			local IsCompressed = false
			if ShouldCompress then
				local EstimatedSize = Compression.EstimateSize(ReplayedData :: any)
				if EstimatedSize > CompressionThreshold then
					local CompressResult = Compression.Compress(ReplayedData :: any, CompressionLevel)
					if CompressResult.Passed then
						PayloadData = (CompressResult :: Types.SuccessResult<string>).Value :: any
						IsCompressed = true
					end
				end
			end

			local OptimisticEnvelope : Envelope<DataType> = {
				_v = SchemaDefinition.Version,
				_c = if IsCompressed then true else nil,
				_d = PayloadData,
			}
			if CurrentLockManager then
				local CurrentLockInfo = CurrentLockManager:ToLockInfo(Key)
				if CurrentLockInfo then OptimisticEnvelope._lock = CurrentLockInfo end
			end

			DidCommit = true
			local UserIds = TargetSession:GetUserIds()
			local Metadata = { SchemaVersion = tostring(SchemaDefinition.Version) }
			return OptimisticEnvelope :: any, UserIds, Metadata
		end)
		return true
	end, { MaxRetries = self._Configuration.MaxRetries, BudgetType = "UpdateAsync" } :: any)

	self._LastWriteTime[Key] = os.clock()

	if not SaveResult.Passed then
		TargetSession:SetStatus("Active")
		return Errors.Fail((SaveResult :: Types.ErrorResult).Error)
	end

	if not DidCommit then
		TargetSession:SetStatus("Active")
		return Errors.Fail(Errors.New("Internal",
			`Optimistic save for '{Key}' was aborted: remote data is sharded or unreadable.`, { Key = Key }))
	end

	local CurrentActions = TargetSession._PendingActions
	if #CurrentActions > ReplayedActionCount then
		local SurvivorActions : { (Data : DataType) -> DataType? } = {}
		for Index = ReplayedActionCount + 1, #CurrentActions do
			table.insert(SurvivorActions, CurrentActions[Index])
		end
		TargetSession._PendingActions = SurvivorActions
	else
		TargetSession._PendingActions = {}
		TargetSession._Dirty = false
	end
	TargetSession._LastSaveTime = os.clock()
	TargetSession._SaveCount += 1
	TargetSession._Status = "Active" :: SessionStatus
	TargetSession.OnSave:Fire(TargetSession._Data)

	return Errors.Passed(true)
end


function Store.Transact<DataType>(
	self : StoreObject<DataType>,
	Participants : { { Key : string, Transform : (Data : DataType) -> DataType? } },
	Configuration : TransactionConfiguration?
) : TransactionResult
	local TransactionInstance = Transaction.New(Configuration)
	for _, ParticipantEntry in Participants do
		TransactionInstance:Add(self, ParticipantEntry.Key, ParticipantEntry.Transform :: any)
	end

	if self._Configuration.Strict then
		local StrictValidationResult = TransactionInstance:ValidateStrict(
			function(StoreReference : unknown, Key : string) : unknown?
				local TargetSession = (StoreReference :: StoreObject<DataType>)._Sessions[Key]
				if TargetSession then return TargetSession._Data end
				return nil
			end)
		if not StrictValidationResult.Passed then
			return { Passed = false, Error = (StrictValidationResult :: Types.ErrorResult).Error, Results = {} }
		end
	end

	local TransactionParticipants = TransactionInstance:GetParticipants()
	for Index, ParticipantData in TransactionParticipants do
		local TargetSession = self._Sessions[ParticipantData.Key]
		if TargetSession then
			TransactionInstance:SetSnapshot(Index, Validation.DeepClone(TargetSession._Data))
		end
	end

	return TransactionInstance:ExecuteOptimistic(
		function(StoreReference : unknown, Key : string, Transform : (Data : unknown) -> unknown?) : Result<unknown>
			local TypedStore = StoreReference :: StoreObject<DataType>
			return TypedStore:Update(Key, Transform :: any) :: any
		end,
		function(StoreReference : unknown, Key : string, Snapshot : unknown) : Result<boolean>
			local TypedStore = StoreReference :: StoreObject<DataType>
			local TargetSession = TypedStore._Sessions[Key]
			if TargetSession then
				TargetSession._Data = Snapshot :: DataType
				TargetSession._Dirty = true
				local PersistResult = TypedStore:Save(Key)
				if not PersistResult.Passed then
					warn(`[Nexus] Transaction: rollback persist failed for '{Key}'`)
				end
			end
			return Errors.Passed(true)
		end)
end

function Store.ListVersions<DataType>(self : StoreObject<DataType>, Key : string, Options : ListVersionsOptions?) : Result<{ VersionInfo }>
	local ResolvedOptions : ListVersionsOptions = (Options or {}) :: any
	local ListResult = Retry.WrapPcall("DataStoreError", function()
		return (self._DataStore :: DataStore):ListVersionsAsync(
			Key,
			ResolvedOptions.SortDirection or Enum.SortDirection.Descending,
			ResolvedOptions.MinDate,
			ResolvedOptions.MaxDate,
			ResolvedOptions.PageSize or 10)
	end, { MaxRetries = 3, BudgetType = "ListAsync" } :: any)

	if not ListResult.Passed then return Errors.Fail((ListResult :: Types.ErrorResult).Error) end

	local Pages = (ListResult :: Types.SuccessResult<DataStoreVersionPages>).Value
	local Versions : { VersionInfo } = {}
	local PageLimit = ResolvedOptions.MaxPages or 100
	local PageCount = 0

	while true do
		PageCount += 1
		for _, Item in Pages:GetCurrentPage() do
			table.insert(Versions, { Version = Item.Version, CreatedTime = Item.CreatedTime, IsDeleted = Item.IsDeleted } :: VersionInfo)
		end
		if Pages.IsFinished or PageCount >= PageLimit then break end
		local AdvanceSuccess, AdvanceError = pcall(function() Pages:AdvanceToNextPageAsync(); return nil :: any end)
		if not AdvanceSuccess then warn(`[Nexus] ListVersions: page advance failed: {AdvanceError}`); break end
	end

	return Errors.Passed(Versions)
end

function Store.GetVersion<DataType>(self : StoreObject<DataType>, Key : string, VersionId : string) : Result<VersionEntry<DataType>>
	local FetchResult = Retry.WrapPcall("DataStoreError", function()
		return (self._DataStore :: DataStore):GetVersionAsync(Key, VersionId)
	end, { MaxRetries = 3, BudgetType = "GetVersionAsync" } :: any)

	if not FetchResult.Passed then return Errors.Fail((FetchResult :: Types.ErrorResult).Error) end
	local RawVersionData = (FetchResult :: Types.SuccessResult<unknown>).Value

	if typeof(RawVersionData) == "table" then
		local VersionTable = RawVersionData :: any
		if typeof(VersionTable._s) == "number" and VersionTable._s > 0 then
			return Errors.Fail(Errors.New("Internal",
				`Version '{VersionId}' of key '{Key}' was sharded. Cannot read or rollback.`, { Key = Key }))
		end
	end

	local UnpackResult = SessionModule.UnpackEnvelope(RawVersionData, self._Configuration.Schema)
	if not UnpackResult.Passed then return Errors.Fail((UnpackResult :: Types.ErrorResult).Error) end
	local UnpackedData = (UnpackResult :: Types.SuccessResult<{ Data : DataType, Version : number }>).Value
	return Errors.Passed({ Version = VersionId, CreatedTime = 0, Data = UnpackedData.Data, IsDeleted = RawVersionData == nil })
end

function Store.Rollback<DataType>(self : StoreObject<DataType>, Key : string, VersionId : string) : Result<DataType>
	local VersionResult = self:GetVersion(Key, VersionId)
	if not VersionResult.Passed then return Errors.Fail((VersionResult :: Types.ErrorResult).Error) end
	local RestoredData = (VersionResult :: Types.SuccessResult<VersionEntry<DataType>>).Value.Data

	local TargetSession = self._Sessions[Key]
	if TargetSession then
		local Before = TargetSession._Data
		TargetSession._Data = RestoredData
		TargetSession._Dirty = true
		self:_RecordAudit(Key, "Rollback", Before, RestoredData)
		local SaveResult = self:Save(Key)
		if not SaveResult.Passed then return Errors.Fail((SaveResult :: Types.ErrorResult).Error) end
		return Errors.Passed(RestoredData)
	end

	if (self._ShardCounts[Key] or 0) == 0 then
		local PeekSuccess, PeekData = pcall(function() return self._DataStore:GetAsync(Key) end)
		if PeekSuccess and typeof(PeekData) == "table" then
			local MaybeShard = (PeekData :: any)._s
			if typeof(MaybeShard) == "number" and MaybeShard > 0 then self._ShardCounts[Key] = MaybeShard end
		end
	end

	local PayloadData : DataType | string = RestoredData
	local IsCompressed = false
	if self._Configuration.Compress then
		local EstimatedSize = Compression.EstimateSize(RestoredData :: any)
		if EstimatedSize > (self._Configuration.CompressionThreshold or 4096) then
			local CompressResult = Compression.Compress(RestoredData :: any, self._Configuration.CompressionLevel or 3)
			if CompressResult.Passed then
				PayloadData = (CompressResult :: Types.SuccessResult<string>).Value :: any
				IsCompressed = true
			end
		end
	end

	local RollbackEnvelope : Envelope<DataType> = {
		_v = self._Configuration.Schema.Version,
		_c = if IsCompressed then true else nil,
		_d = PayloadData,
	}
	local UserIds : { number }? = nil
	if self._Configuration.GetUserIds then
		UserIds = self._Configuration.GetUserIds(Key)
	else
		local ParsedUserId = tonumber(Key)
		if ParsedUserId then UserIds = { ParsedUserId } end
	end

	local WriteResult = self:_WriteEnvelope(Key, RollbackEnvelope, UserIds)
	if not WriteResult.Passed then return Errors.Fail((WriteResult :: Types.ErrorResult).Error) end
	self:_RecordAudit(Key, "Rollback", nil, RestoredData)
	return Errors.Passed(RestoredData)
end

function Store.ListKeys<DataType>(self : StoreObject<DataType>, Prefix : string?, PageSize : number?, MaxPages : number?) : Result<{ string }>
	local ListResult = Retry.WrapPcall("DataStoreError", function()
		return (self._DataStore :: DataStore):ListKeysAsync(Prefix, PageSize or 100)
	end, { MaxRetries = 3, BudgetType = "ListAsync" } :: any)

	if not ListResult.Passed then return Errors.Fail((ListResult :: Types.ErrorResult).Error) end

	local Pages = (ListResult :: Types.SuccessResult<DataStoreKeyPages>).Value
	local Keys : { string } = {}
	local PageLimit = MaxPages or 100
	local PageCount = 0

	while true do
		PageCount += 1
		for _, Item in Pages:GetCurrentPage() do
			if not string.find(Item.KeyName, "_shard_%d+$") then
				table.insert(Keys, Item.KeyName :: string)
			end
		end
		if Pages.IsFinished or PageCount >= PageLimit then break end
		local AdvanceSuccess, AdvanceError = pcall(function() Pages:AdvanceToNextPageAsync(); return nil :: any end)
		if not AdvanceSuccess then warn(`[Nexus] ListKeys: page advance failed: {AdvanceError}`); break end
	end

	return Errors.Passed(Keys)
end

function Store.ListKeysPage<DataType>(self : StoreObject<DataType>, Prefix : string?, PageSize : number?) : Result<PageCursor>
	local ListResult = Retry.WrapPcall("DataStoreError", function()
		return (self._DataStore :: DataStore):ListKeysAsync(Prefix, PageSize or 100)
	end, { MaxRetries = 3, BudgetType = "ListAsync" } :: any)

	if not ListResult.Passed then return Errors.Fail((ListResult :: Types.ErrorResult).Error) end

	local Pages = (ListResult :: Types.SuccessResult<DataStoreKeyPages>).Value
	return Errors.Passed({
		_Pages = Pages,
		_Done = Pages.IsFinished,
		_PageCount = 0,
		_MaxPages = 1000,
	} :: PageCursor)
end

function Store.NextPage<DataType>(self : StoreObject<DataType>, Cursor : PageCursor) : Result<{ string }>
	if Cursor._Done then return Errors.Passed({}) end
	local Pages = Cursor._Pages :: DataStoreKeyPages

	if Cursor._PageCount > 0 then
		local AdvanceSuccess, AdvanceError = pcall(function() Pages:AdvanceToNextPageAsync(); return nil :: any end)
		if not AdvanceSuccess then
			return Errors.Fail(Errors.New("DataStoreError", `Page advance failed: {AdvanceError}`))
		end
	end

	Cursor._PageCount += 1
	local Keys : { string } = {}
	for _, Item in Pages:GetCurrentPage() do
		if not string.find(Item.KeyName, "_shard_%d+$") then
			table.insert(Keys, Item.KeyName :: string)
		end
	end

	Cursor._Done = Pages.IsFinished or Cursor._PageCount >= Cursor._MaxPages
	return Errors.Passed(Keys)
end


function Store.SoftDelete<DataType>(self : StoreObject<DataType>, Key : string) : Result<boolean>
	if self._Sessions[Key] then
		self:Close(Key)
	end

	local StartTime = os.clock()
	local FetchResult = Retry.WrapPcall("DataStoreError", function()
		return self._DataStore:GetAsync(Key)
	end, { MaxRetries = 3, BudgetType = "GetAsync" } :: any)

	if not FetchResult.Passed then
		return Errors.Fail((FetchResult :: Types.ErrorResult).Error)
	end

	local RawData = (FetchResult :: Types.SuccessResult<unknown>).Value
	self._SoftDeleted[Key] = { DeletedAt = os.time(), EnvelopeSnapshot = RawData }

	local DeleteEnvelope = {
		_v = self._Configuration.Schema.Version,
		_d = nil :: any,
		_deleted = true,
		_deletedAt = os.time(),
	}

	local WriteResult = Retry.WrapPcall("DataStoreError", function()
		self._DataStore:UpdateAsync(Key, function()
			return DeleteEnvelope :: any
		end)
		return true
	end, { MaxRetries = 3, BudgetType = "UpdateAsync" } :: any)

	self._Telemetry:Record("Store.SoftDelete", os.clock() - StartTime, WriteResult.Passed)
	self:_RecordAudit(Key, "SoftDelete", RawData, nil)

	if not WriteResult.Passed then
		self._SoftDeleted[Key] = nil
		return Errors.Fail((WriteResult :: Types.ErrorResult).Error)
	end

	return Errors.Passed(true)
end

function Store.Recover<DataType>(self : StoreObject<DataType>, Key : string) : Result<DataType>
	local DeletedEntry = self._SoftDeleted[Key]
	if not DeletedEntry then
		return Errors.Fail(Errors.New("KeyNotFound",
			`Key '{Key}' is not in the soft-delete registry on this server`, { Key = Key }))
	end

	local StartTime = os.clock()
	local Snapshot = DeletedEntry.EnvelopeSnapshot

	local WriteResult = Retry.WrapPcall("DataStoreError", function()
		self._DataStore:UpdateAsync(Key, function()
			return Snapshot :: any
		end)
		return true
	end, { MaxRetries = 3, BudgetType = "UpdateAsync" } :: any)

	if not WriteResult.Passed then
		self._Telemetry:Record("Store.Recover", os.clock() - StartTime, false)
		return Errors.Fail((WriteResult :: Types.ErrorResult).Error)
	end

	self._SoftDeleted[Key] = nil
	self:_RecordAudit(Key, "Recover", nil, Snapshot)

	local LoadResult = self:Load(Key)
	self._Telemetry:Record("Store.Recover", os.clock() - StartTime, LoadResult.Passed)
	return LoadResult
end

function Store.IsSoftDeleted<DataType>(self : StoreObject<DataType>, Key : string) : boolean
	return self._SoftDeleted[Key] ~= nil
end

function Store.ListSoftDeleted<DataType>(self : StoreObject<DataType>) : { SoftDeleteInfo }
	local Entries : { SoftDeleteInfo } = {}
	local Retention = self._Configuration.SoftDeleteRetention or DEFAULT_SOFT_DELETE_RETENTION
	for Key, Entry in self._SoftDeleted do
		table.insert(Entries, {
			Key = Key,
			DeletedAt = Entry.DeletedAt,
			ExpiresAt = if Retention > 0 then Entry.DeletedAt + Retention else nil,
		} :: SoftDeleteInfo)
	end
	return Entries
end

function Store.PurgeSoftDeleted<DataType>(self : StoreObject<DataType>, Key : string) : Result<boolean>
	if not self._SoftDeleted[Key] then
		return Errors.Fail(Errors.New("KeyNotFound",
			`Key '{Key}' is not in the soft-delete registry`, { Key = Key }))
	end

	local ShardCount = 0
	local PeekSuccess, PeekData = pcall(function() return self._DataStore:GetAsync(Key) end)
	if PeekSuccess and typeof(PeekData) == "table" then
		local MaybeShard = (PeekData :: any)._s
		if typeof(MaybeShard) == "number" and MaybeShard > 0 then
			ShardCount = MaybeShard
		end
	end

	if ShardCount > 0 then
		Shard.DeleteAllShards(self._DataStore, Key, ShardCount)
	end

	local RemoveResult = Retry.WrapPcall("DataStoreError", function()
		self._DataStore:RemoveAsync(Key)
		return true
	end, { MaxRetries = 3, BudgetType = "SetIncrementAsync" } :: any)

	if RemoveResult.Passed then
		self._SoftDeleted[Key] = nil
		self._AuditLogs[Key] = nil
		self:_RecordAudit(Key, "PurgeSoftDeleted", nil, nil)
	end

	return RemoveResult
end

function Store.GetAuditLog<DataType>(self : StoreObject<DataType>, Key : string) : { AuditEntry }
	return self._AuditLogs[Key] or {}
end

function Store.GetTelemetry<DataType>(self : StoreObject<DataType>) : TelemetryScope
	return self._Telemetry
end

function Store.Use<DataType>(self : StoreObject<DataType>, Phase : MiddlewarePhase, Handler : MiddlewareFn) : ()
	self._Middleware:Use(Phase, Handler)
end

function Store.SetCallbacks<DataType>(self : StoreObject<DataType>, Callbacks : StoreCallbacks<DataType>) : ()
	self._Callbacks = Callbacks
end

function Store.GetSessionInfo<DataType>(self : StoreObject<DataType>, Key : string) : SessionInfo?
	local TargetSession = self._Sessions[Key]
	if not TargetSession then return nil end
	return TargetSession:GetInfo()
end

function Store.GetCircuitInfo<DataType>(self : StoreObject<DataType>) : CircuitInfo?
	if not self._LockManager then return nil end
	return self._LockManager:GetCircuit():GetInfo()
end

function Store.GetActiveKeys<DataType>(self : StoreObject<DataType>) : { string }
	local Keys : { string } = {}
	for Key, _ in self._Sessions do
		table.insert(Keys, Key)
	end
	return Keys
end

function Store.IsLoaded<DataType>(self : StoreObject<DataType>, Key : string) : boolean
	return self._Sessions[Key] ~= nil
end

function Store.Remove<DataType>(self : StoreObject<DataType>, Key : string) : Result<boolean>
	if self._Sessions[Key] then self:Close(Key) end

	local ShardCount = self._ShardCounts[Key] or 0
	if ShardCount == 0 then
		local PeekSuccess, PeekData = pcall(function() return self._DataStore:GetAsync(Key) end)
		if PeekSuccess and typeof(PeekData) == "table" then
			local MaybeShard = (PeekData :: any)._s
			if typeof(MaybeShard) == "number" and MaybeShard > 0 then ShardCount = MaybeShard end
		end
	end

	if ShardCount > 0 then
		Shard.DeleteAllShards(self._DataStore, Key, ShardCount)
	end
	self._ShardCounts[Key] = nil
	self._AuditLogs[Key] = nil
	self._SoftDeleted[Key] = nil

	return Retry.WrapPcall("DataStoreError", function()
		self._DataStore:RemoveAsync(Key)
		return true
	end, { MaxRetries = 3, BudgetType = "SetIncrementAsync" } :: any)
end

function Store.Destroy<DataType>(self : StoreObject<DataType>) : ()
	self:CloseAll()
	if self._IdleThread then task.cancel(self._IdleThread); self._IdleThread = nil end
	for Key, Thread in self._DebounceTimers do
		task.cancel(Thread)
	end
	table.clear(self._DebounceTimers)
	Lifecycle.Unregister(self)
	Messaging.Unsubscribe(self._Configuration.Name)
	self.OnLoad:Destroy()
	self.OnSave:Destroy()
	self.OnRelease:Destroy()
	self.OnError:Destroy()
	self.OnDirty:Destroy()
	self.OnIdle:Destroy()
end

return Store
