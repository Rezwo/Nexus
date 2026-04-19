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
local _Transaction = require(script.Parent.Transaction)
local Shard = require(script.Parent.Shard)
local Compression = require(script.Parent.Compression)
local Lifecycle = require(script.Parent.Lifecycle)
local Messaging = require(script.Parent.Messaging)
local Semaphore = require(script.Parent.Semaphore)
local Telemetry = require(script.Parent.Telemetry)
local MiddlewareModule = require(script.Parent.Middleware)
local Budget = require(script.Parent.Budget)

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
local DEFAULT_AUDIT_SHARD_THRESHOLD = 3_500_000
local DEFAULT_SOFT_DELETE_RETENTION = 2592000
local DEFAULT_CLOSE_SAVE_MAX_RETRIES = 3

local HttpService = game:GetService("HttpService")

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
	_AuditShardCounts : { [string] : number },
	_AuditDirty : { [string] : boolean },
	_SoftDeleted : { [string] : { DeletedAt : number, EnvelopeSnapshot : unknown } },
	_DebounceTimers : { [string] : thread },
	_LastActivity : { [string] : number },
	_LastKnownTxv : { [string] : number },
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
	QueueAction : (self : StoreObject<DataType>, Key : string, Action : (Data : DataType) -> DataType?) -> Result<boolean>,
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
	NextPage : (self : StoreObject<DataType>, Cursor : PageCursor) -> Result<{ Keys : { string }, Done : boolean }>,
	SoftDelete : (self : StoreObject<DataType>, Key : string) -> Result<boolean>,
	Recover : (self : StoreObject<DataType>, Key : string) -> Result<DataType>,
	IsSoftDeleted : (self : StoreObject<DataType>, Key : string) -> boolean,
	ListSoftDeleted : (self : StoreObject<DataType>) -> Result<{ SoftDeleteInfo }>,
	PurgeSoftDeleted : (self : StoreObject<DataType>, Key : string) -> Result<boolean>,
	GetAuditLog : (self : StoreObject<DataType>, Key : string) -> Result<{ AuditEntry }>,
	GetAuditLogRemote : (self : StoreObject<DataType>, Key : string) -> Result<{ AuditEntry }>,
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
	_PerformDebouncedSave : (self : StoreObject<DataType>, Key : string) -> Result<boolean>,
	_FinalizeSave : (self : StoreObject<DataType>, Key : string, TargetSession : SessionModule.SessionObject<DataType>, SaveEnvelope : Envelope<DataType>, UserIds : { number }?, MiddlewareData : DataType) -> Result<boolean>,
	_CommitParticipantCAS : (self : StoreObject<DataType>, Key : string, Transform : (Data : DataType) -> DataType?, ExpectedTxv : number, UserIds : { number }?) -> Result<boolean>,
	_RecordAudit : (self : StoreObject<DataType>, Key : string, Operation : string, Before : unknown?, After : unknown?) -> (),
	_FlushAuditLog : (self : StoreObject<DataType>, Key : string) -> (),
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

	-- Validate migration chain covers every version from 2 → Schema.Version
	-- with no gaps or duplicates, so runtime migration can never silently skip
	-- a step.
	if Configuration.Schema.Version > 1 then
		local Migrations = Configuration.Schema.Migrations
		assert(Migrations and #Migrations > 0,
			`Schema version is {Configuration.Schema.Version} but no migration steps are defined`)
		local CoveredVersions : { [number] : boolean } = {}
		for _, Step in Migrations do
			assert(typeof(Step.Version) == "number" and Step.Version >= 2,
				`Migration step version must be >= 2, got '{tostring(Step.Version)}'`)
			assert(typeof(Step.Up) == "function",
				`Migration step for version {Step.Version} must have an 'Up' function`)
			assert(not CoveredVersions[Step.Version],
				`Duplicate migration step for version {Step.Version}`)
			CoveredVersions[Step.Version] = true
		end
		for V = 2, Configuration.Schema.Version do
			assert(CoveredVersions[V],
				`Schema migration chain is missing step for version {V} (schema version is {Configuration.Schema.Version})`)
		end
	end

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
		AuditPersistence = Configuration.AuditPersistence or false,
		AuditShardThreshold = Configuration.AuditShardThreshold or DEFAULT_AUDIT_SHARD_THRESHOLD,
		Debug = Configuration.Debug or false,
		IdleTimeout = Configuration.IdleTimeout or DEFAULT_IDLE_THRESHOLD,
		IdleCheckInterval = Configuration.IdleCheckInterval
			or (if Configuration.IdleTimeout then math.max(Configuration.IdleTimeout / 2, 0.1) else 60),
		WriteCooldown = Configuration.WriteCooldown or WRITE_COOLDOWN,
		ReadOnly = Configuration.ReadOnly or false,
		CloseSaveMaxRetries = Configuration.CloseSaveMaxRetries or DEFAULT_CLOSE_SAVE_MAX_RETRIES,
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
	self._AuditShardCounts = {}
	self._AuditDirty = {}
	self._SoftDeleted = {}
	self._DebounceTimers = {}
	self._LastActivity = {}
	self._LastKnownTxv = {}
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
	self._AuditDirty[Key] = true
end

function Store._TouchActivity<DataType>(self : StoreObject<DataType>, Key : string) : ()
	self._LastActivity[Key] = os.clock()
end


function Store.Load<DataType>(self : StoreObject<DataType>, Key : string) : Result<DataType>
	local StartTime = os.clock()
	self:_DebugLog("Load.Start", Key)
	if self._Configuration.ReadOnly then
		return Errors.Fail(Errors.New("Internal",
			`Store '{self._Configuration.Name}' is read-only; Load acquires a lock and is not permitted`, { Key = Key }))
	end

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
		if self._Callbacks.OnError then self._Callbacks.OnError(Key, FetchError) end
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
				if self._Callbacks.OnError then self._Callbacks.OnError(Key, ShardError) end
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
		if self._LockManager then
			-- Release the MemoryStore lease so other servers can acquire.
			self._LockManager:Release(Key)
			-- Also stamp the envelope with a released lock so the disk entry
			-- doesn't block future loads for EnvelopeLeaseTimeToLive seconds.
			if RawData ~= nil and typeof(RawData) == "table" then
				local UnlockEnvelope = table.clone(RawData :: any) :: any
				UnlockEnvelope._lock = Lock.ToReleasedLockInfo()
				task.spawn(function()
					pcall(function()
						self._DataStore:UpdateAsync(Key, function(CurrentRemote : unknown)
							if typeof(CurrentRemote) ~= "table" then return nil end
							local Patched = table.clone(CurrentRemote :: any)
							Patched._lock = Lock.ToReleasedLockInfo()
							return Patched
						end)
					end)
				end)
			end
		end
		local UnpackError = (UnpackResult :: Types.ErrorResult).Error
		UnpackError.Key = Key
		self.OnError:Fire(Key, UnpackError)
		if self._Callbacks.OnError then self._Callbacks.OnError(Key, UnpackError) end
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
		local MiddlewareOk, MiddlewareResult = self._Middleware:RunSafe("load", Key, LoadedData)
		if not MiddlewareOk then
			if self._LockManager then self._LockManager:Release(Key) end
			local MwError = Errors.New("Internal",
				`Load middleware threw for key '{Key}': {tostring(MiddlewareResult)}`, { Key = Key })
			self._Telemetry:Record("Store.Load", os.clock() - StartTime, false, MwError)
			return Errors.Fail(MwError)
		end
		if MiddlewareResult ~= nil then
			LoadedData = MiddlewareResult :: DataType
		end
	end

	-- Capture _txv from the raw envelope for CAS transactions.
	local LoadedTxv = 0
	if RawData ~= nil and typeof(RawData) == "table" then
		local RawTxv = (RawData :: any)._txv
		if typeof(RawTxv) == "number" then
			LoadedTxv = RawTxv
		end
	end
	self._LastKnownTxv[Key] = LoadedTxv

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
	self:_RecordAudit(Key, "Load", Validation.DeepClone(LoadedData), LoadedData)

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
		if ManifestTable._deleted == true then
			local DeletedError = Errors.New("KeyNotFound", `Key '{Key}' has been soft-deleted`, { Key = Key })
			self._Telemetry:Record("Store.Peek", os.clock() - StartTime, false, DeletedError)
			return Errors.Fail(DeletedError)
		end
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
	local ReadResult = TargetSession:Read()
	-- Only update activity if the read actually succeeded (P2-10: don't
	-- mark a dead/error session as active to avoid suppressing OnIdle).
	if ReadResult.Passed then
		self:_TouchActivity(Key)
	end
	return ReadResult
end

function Store.Update<DataType>(
	self : StoreObject<DataType>,
	Key : string,
	Transform : (Data : DataType) -> DataType?
) : Result<DataType>
	if self._Configuration.ReadOnly then
		return Errors.Fail(Errors.New("Internal",
			`Store '{self._Configuration.Name}' is read-only; writes are not permitted`, { Key = Key }))
	end
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
			self:_RecordAudit(Key, "Update", Before, Validation.DeepClone(TargetSession._Data))
		end
	else
		local UpdateError = (UpdateResult :: Types.ErrorResult).Error
		self.OnError:Fire(Key, UpdateError)
		if self._Callbacks.OnError then self._Callbacks.OnError(Key, UpdateError) end
	end
	return UpdateResult
end

function Store.QueueAction<DataType>(
	self : StoreObject<DataType>,
	Key : string,
	Action : (Data : DataType) -> DataType?
) : Result<boolean>
	local TargetSession = self._Sessions[Key]
	if not TargetSession then
		return Errors.Fail(Errors.New("SessionNotFound", `No active session for key '{Key}'`, { Key = Key }))
	end
	return TargetSession:QueueAction(Action)
end

function Store.Save<DataType>(self : StoreObject<DataType>, Key : string) : Result<boolean>
	local TargetSession = self._Sessions[Key]
	if not TargetSession then
		return Errors.Fail(Errors.New("SessionNotFound", `No active session for key '{Key}'`, { Key = Key }))
	end

	if not TargetSession:IsDirty() then
		return Errors.Passed(true)
	end

	local DebounceInterval = self._Configuration.DebounceInterval
	if DebounceInterval and DebounceInterval > 0 and not self._ShuttingDown then
		local ExistingTimer = self._DebounceTimers[Key]
		if ExistingTimer then
			task.cancel(ExistingTimer)
		end
		self._DebounceTimers[Key] = task.delay(DebounceInterval, function()
			self._DebounceTimers[Key] = nil
			if self._Sessions[Key] and self._Sessions[Key]:IsDirty() then
				self:_PerformDebouncedSave(Key)
			end
		end)
		return Errors.Passed(true)
	end

	return self:_PerformDebouncedSave(Key)
end

function Store._PerformDebouncedSave<DataType>(self : StoreObject<DataType>, Key : string) : Result<boolean>
	local TargetSession = self._Sessions[Key]
	if not TargetSession then
		return Errors.Fail(Errors.New("SessionNotFound", `No active session for key '{Key}'`, { Key = Key }))
	end

	if TargetSession._Status ~= "Active" then
		return Errors.Passed(true)
	end

	TargetSession:SetStatus("Saving")

	if not self._ShuttingDown then
		self:_WaitForWriteCooldown(Key)
	end

	local SaveDataForMiddleware : DataType = TargetSession._Data

	if self._Callbacks.BeforeSave then
		local DataForWire = self._Callbacks.BeforeSave(Key, Validation.DeepClone(TargetSession._Data))
		if DataForWire == nil then
			TargetSession:SetStatus("Active")
			return Errors.Fail(Errors.New("Internal",
				`BeforeSave cancelled save for '{Key}'`, { Key = Key }))
		end
		local LiveData = TargetSession._Data
		TargetSession._Data = DataForWire
		local EnvelopeResult = TargetSession:BuildEnvelope()
		TargetSession._Data = LiveData
		if not EnvelopeResult.Passed then
			TargetSession:SetStatus("Active")
			return Errors.Fail((EnvelopeResult :: Types.ErrorResult).Error)
		end
		SaveDataForMiddleware = DataForWire
		local SaveEnvelope = (EnvelopeResult :: Types.SuccessResult<Envelope<DataType>>).Value
		local UserIds = TargetSession:GetUserIds()

		if self._LockManager then
			local CurrentLockInfo = self._LockManager:ToLockInfo(Key)
			if CurrentLockInfo then SaveEnvelope._lock = CurrentLockInfo end
		end

		return self:_FinalizeSave(Key, TargetSession, SaveEnvelope, UserIds, SaveDataForMiddleware)
	end

	local EnvelopeResult = TargetSession:BuildEnvelope()
	if not EnvelopeResult.Passed then
		TargetSession:SetStatus("Active")
		return Errors.Fail((EnvelopeResult :: Types.ErrorResult).Error)
	end

	local SaveEnvelope = (EnvelopeResult :: Types.SuccessResult<Envelope<DataType>>).Value
	local UserIds = TargetSession:GetUserIds()

	if self._LockManager then
		local CurrentLockInfo = self._LockManager:ToLockInfo(Key)
		if CurrentLockInfo then SaveEnvelope._lock = CurrentLockInfo end
	end

	return self:_FinalizeSave(Key, TargetSession, SaveEnvelope, UserIds, SaveDataForMiddleware)
end

function Store._FinalizeSave<DataType>(
	self : StoreObject<DataType>,
	Key : string,
	TargetSession : SessionModule.SessionObject<DataType>,
	SaveEnvelope : Envelope<DataType>,
	UserIds : { number }?,
	MiddlewareData : DataType
) : Result<boolean>
	if self._Middleware:HasMiddleware("save") then
		local MiddlewareOk, MiddlewareResult = self._Middleware:RunSafe("save", Key, MiddlewareData)
		if not MiddlewareOk then
			TargetSession:SetStatus("Active")
			local MwError = Errors.New("Internal",
				`Save middleware threw for key '{Key}': {tostring(MiddlewareResult)}`, { Key = Key })
			self.OnError:Fire(Key, MwError)
			if self._Callbacks.OnError then self._Callbacks.OnError(Key, MwError) end
			return Errors.Fail(MwError)
		end
	end

	local WriteResult = self:_WriteEnvelope(Key, SaveEnvelope, UserIds)
	self._LastWriteTime[Key] = os.clock()

	if not WriteResult.Passed then
		TargetSession:SetStatus("Active")
		local SaveError = (WriteResult :: Types.ErrorResult).Error
		SaveError.Key = Key
		self.OnError:Fire(Key, SaveError)
		if self._Callbacks.OnError then self._Callbacks.OnError(Key, SaveError) end
		return Errors.Fail(SaveError)
	end

	TargetSession:SetStatus("Active")
	TargetSession:MarkSaved()
	self:_TouchActivity(Key)
	self:_RecordAudit(Key, "Save", Validation.DeepClone(TargetSession._Data), TargetSession._Data)
	self:_FlushAuditLog(Key)

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
	self:_FlushAuditLog(Key)
	TargetSession:Close()
	self._Sessions[Key] = nil
	self._LastWriteTime[Key] = nil
	self._ShardCounts[Key] = nil
	self._LastActivity[Key] = nil
	self._LastKnownTxv[Key] = nil
	self._AuditLogs[Key] = nil
	self._AuditShardCounts[Key] = nil
	self._AuditDirty[Key] = nil

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
		warn(`[Nexus] Close-save envelope build failed for '{Key}': {(EnvelopeResult :: Types.ErrorResult).Error.Message}`)
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

	local MaxAttempts = self._Configuration.CloseSaveMaxRetries or DEFAULT_CLOSE_SAVE_MAX_RETRIES
	local Delay = 1
	local WriteResult : Result<boolean>? = nil

	for Attempt = 1, MaxAttempts do
		WriteResult = self:_WriteEnvelope(Key, CloseEnvelope, UserIds)
		self._LastWriteTime[Key] = os.clock()
		if (WriteResult :: Result<boolean>).Passed then
			break
		end
		if Attempt < MaxAttempts then
			task.wait(Delay)
			Delay = math.min(Delay * 2, 8)
		end
	end

	if not (WriteResult :: Result<boolean>).Passed then
		local WriteError = ((WriteResult :: any) :: Types.ErrorResult).Error
		warn(`[Nexus] Close-save failed for '{Key}' after {MaxAttempts} attempts: {WriteError.Message}`)
		-- Do NOT mark dirty=false — session is about to be destroyed anyway,
		-- but leaving it dirty means audit flush is also skipped, preventing
		-- a confusing "saved" audit entry for data that wasn't persisted.
		self.OnError:Fire(Key, WriteError)
		if self._Callbacks.OnError then self._Callbacks.OnError(Key, WriteError) end
	else
		TargetSession:MarkSaved()
		self:_FlushAuditLog(Key)
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
		while not self._ShuttingDown do
			task.wait(self._Configuration.AutoSaveInterval or 300)
			if self._ShuttingDown then break end
			self:SaveAll()
		end
	end)
end

function Store._StartIdleTracker<DataType>(self : StoreObject<DataType>) : ()
	self._IdleThread = task.spawn(function()
		local IdleFired : { [string] : boolean } = {}
		local CheckInterval = self._Configuration.IdleCheckInterval or 60
		local IdleTimeout = self._Configuration.IdleTimeout or DEFAULT_IDLE_THRESHOLD
		while true do
			task.wait(CheckInterval)
			local CurrentTime = os.clock()
			for Key, LastActive in self._LastActivity do
				local IdleTime = CurrentTime - LastActive
				if IdleTime >= IdleTimeout then
					if not IdleFired[Key] then
						IdleFired[Key] = true
						self.OnIdle:Fire(Key, IdleTime)
					end
				else
					IdleFired[Key] = nil
				end
			end
			-- Prune stale IdleFired entries for sessions that have since been
			-- closed (removed from _LastActivity) to prevent unbounded growth.
			for Key in IdleFired do
				if self._LastActivity[Key] == nil then
					IdleFired[Key] = nil
				end
			end

			-- Sweep expired soft-deleted entries and purge them from disk so the
			-- DataStore key doesn't stay permanently tombstoned past retention.
			local RetentionPeriod = self._Configuration.SoftDeleteRetention
			if RetentionPeriod and RetentionPeriod > 0 then
				local ExpiredKeys : { string } = {}
				local Now = os.time()
				for SoftKey, Entry in self._SoftDeleted do
					if Now - Entry.DeletedAt >= RetentionPeriod then
						table.insert(ExpiredKeys, SoftKey)
					end
				end
				for _, SoftKey in ExpiredKeys do
					task.spawn(function()
						local PurgeResult = self:PurgeSoftDeleted(SoftKey)
						if not PurgeResult.Passed then
							warn(`[Nexus] Soft-delete expiry purge failed for '{SoftKey}': {(PurgeResult :: Types.ErrorResult).Error.Message}`)
						end
					end)
				end
			end
		end
	end)
end

function Store.SaveAll<DataType>(self : StoreObject<DataType>) : ()
	for Key, TargetSession in self._Sessions do
		if TargetSession:IsDirty() and TargetSession._Status == "Active" then
			if not Budget.HasCapacity("UpdateAsync") then
				-- Budget is exhausted; skip this key this cycle. The next auto-save
				-- interval will retry. This prevents auto-save from queuing writes
				-- that would immediately block for up to MAXIMUM_QUEUE_WAIT seconds.
				continue
			end
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
		local Cooldown = self._Configuration.WriteCooldown or WRITE_COOLDOWN
		local ElapsedTime = os.clock() - LastWriteTimestamp
		if ElapsedTime < Cooldown then
			task.wait(Cooldown - ElapsedTime)
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
			self._DataStore:UpdateAsync(Key, function(CurrentRemote : unknown)
				-- Inherit and increment the remote transaction version.
				local RemoteTransactionVersion = 0
				if typeof(CurrentRemote) == "table" then
					local RemoteTable = CurrentRemote :: any
					if typeof(RemoteTable._txv) == "number" then
						RemoteTransactionVersion = RemoteTable._txv
					end
				end
				local NewManifest = table.clone(Manifest :: any)
				NewManifest._txv = RemoteTransactionVersion + 1
				self._LastKnownTxv[Key] = RemoteTransactionVersion + 1
				local Metadata = { SchemaVersion = tostring(Manifest._v) }
				return NewManifest, UserIds or {}, Metadata
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

	-- Pre-write size guard: reject obviously over-limit payloads before spending
	-- a DataStore API call that will fail with a quota error anyway.
	local PayloadForSize = WriteEnvelope._d
	local EstimatedPayloadSize = if typeof(PayloadForSize) == "string"
		then #(PayloadForSize :: string)
		else Compression.EstimateSize(PayloadForSize :: any)
	if EstimatedPayloadSize > 4_000_000 then
		local SizeError = Errors.New("ValidationFailed",
			`Payload for key '{Key}' is {EstimatedPayloadSize} bytes, exceeding the 4 MB DataStore limit. Enable Compress or ShardThreshold.`,
			{ Key = Key })
		self._Telemetry:Record("Store.Write.SizeRejected", 0, false, SizeError)
		return Errors.Fail(SizeError)
	end

	local SaveResult = Retry.WrapPcall("DataStoreError", function()
		self._DataStore:UpdateAsync(Key, function(CurrentRemote : unknown)
			-- Inherit and increment the remote transaction version.
			-- This makes _txv a reliable CAS token even outside explicit transactions.
			local RemoteTransactionVersion = 0
			if typeof(CurrentRemote) == "table" then
				local RemoteTable = CurrentRemote :: any
				if typeof(RemoteTable._txv) == "number" then
					RemoteTransactionVersion = RemoteTable._txv
				end
			end
			local NewEnvelope = table.clone(WriteEnvelope :: any)
			NewEnvelope._txv = RemoteTransactionVersion + 1
			self._LastKnownTxv[Key] = RemoteTransactionVersion + 1
			local Metadata = { SchemaVersion = tostring(WriteEnvelope._v) }
			return NewEnvelope, UserIds or {}, Metadata
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
		-- Sharded payloads can't participate in the remote-replay CAS path because
		-- the shard manifest carries no meaningful version token. The in-memory data
		-- is already correct (pending actions are applied), so a regular save is
		-- semantically identical and handles shards natively.
		return self:_PerformDebouncedSave(Key)
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
				-- TryCompress does a single JSON encode for both size estimation and
				-- compression, avoiding the double-encode of EstimateSize + Compress.
				local Compressed, WasCompressed, JsonSize = Compression.TryCompress(
					ReplayedData :: any, CompressionThreshold, CompressionLevel)
				if WasCompressed then
					PayloadData = Compressed :: any
					IsCompressed = true
				elseif JsonSize > 4_000_000 then
					warn(`[Nexus] SaveOptimistic: payload ({JsonSize} bytes) exceeds 4 MB for '{Key}'. Aborting commit.`)
					return nil
				end
			else
				local EstimatedSize = Compression.EstimateSize(ReplayedData :: any)
				if EstimatedSize > 4_000_000 then
					warn(`[Nexus] SaveOptimistic: uncompressed payload ({EstimatedSize} bytes) exceeds 4 MB for '{Key}'. Aborting commit.`)
					return nil
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
	local HasSurvivors = #CurrentActions > ReplayedActionCount
	if HasSurvivors then
		local SurvivorActions : { (Data : DataType) -> DataType? } = {}
		for Index = ReplayedActionCount + 1, #CurrentActions do
			table.insert(SurvivorActions, CurrentActions[Index])
		end
		TargetSession._PendingActions = SurvivorActions
		-- Survivors mean the session still has uncommitted actions on disk;
		-- keep it dirty so the next save cycle picks them up.
		TargetSession._Dirty = true
	else
		TargetSession._PendingActions = {}
		TargetSession._Dirty = false
	end
	TargetSession._LastSaveTime = os.clock()
	TargetSession._SaveCount += 1
	TargetSession._Status = "Active" :: SessionStatus
	self:_TouchActivity(Key)

	-- Only fire OnSave when the full pending set was committed. With survivors
	-- outstanding, callers that rely on OnSave meaning "all data durable" would
	-- be misled.
	if not HasSurvivors then
		TargetSession.OnSave:Fire(TargetSession._Data)
		self.OnSave:Fire(Key, TargetSession._Data)
		if self._Callbacks.OnSave then self._Callbacks.OnSave(Key, TargetSession._Data) end
	end

	return Errors.Passed(true)
end


function Store._CommitParticipantCAS<DataType>(
	self : StoreObject<DataType>,
	Key : string,
	Transform : (Data : DataType) -> DataType?,
	ExpectedTransactionVersion : number,
	UserIds : { number }?
) : Result<boolean>
	local SchemaDefinition = self._Configuration.Schema
	local ShouldCompress = self._Configuration.Compress or false
	local CompressionThreshold = self._Configuration.CompressionThreshold or 4096
	local CompressionLevel = self._Configuration.CompressionLevel or 3
	local CurrentLockManager = self._LockManager

	local WasConflict = false
	local TransformReturnedNil = false
	local ValidationFailure : string? = nil

	local WriteResult = Retry.WrapPcall("DataStoreError", function()
		self._DataStore:UpdateAsync(Key, function(CurrentRemote : unknown)
			WasConflict = false
			TransformReturnedNil = false


			local RemoteTransactionVersion = 0
			if typeof(CurrentRemote) == "table" then
				local RemoteTable = CurrentRemote :: any
				if typeof(RemoteTable._txv) == "number" then
					RemoteTransactionVersion = RemoteTable._txv
				end
			end
			if RemoteTransactionVersion ~= ExpectedTransactionVersion then
				WasConflict = true
				return nil
			end

			local UnpackResult = SessionModule.UnpackEnvelope(CurrentRemote, SchemaDefinition)
			if not UnpackResult.Passed then
				WasConflict = true
				return nil
			end
			local RemoteData = (UnpackResult :: Types.SuccessResult<{ Data : DataType, Version : number }>).Value.Data

			local TransformedData = Transform(RemoteData)
			if TransformedData == nil then
				TransformReturnedNil = true
				return nil
			end


			if SchemaDefinition.Validate then
				local IsValid, ValidationReason = SchemaDefinition.Validate(TransformedData)
				if not IsValid then
					ValidationFailure = ValidationReason or "validation failed"
					return nil
				end
			end


			local PayloadData : DataType | string = TransformedData
			local IsCompressed = false
			if ShouldCompress then
				local Compressed, WasCompressed, _ = Compression.TryCompress(
					TransformedData :: any, CompressionThreshold, CompressionLevel)
				if WasCompressed then
					PayloadData = Compressed :: any
					IsCompressed = true
				end
			end

			local CommitEnvelope : Types.Envelope<DataType> = {
				_v = SchemaDefinition.Version,
				_txv = RemoteTransactionVersion + 1,
				_c = if IsCompressed then true else nil,
				_d = PayloadData,
			}
			if CurrentLockManager then
				local CurrentLockInfo = CurrentLockManager:ToLockInfo(Key)
				if CurrentLockInfo then CommitEnvelope._lock = CurrentLockInfo end
			end

			self._LastKnownTxv[Key] = RemoteTransactionVersion + 1
			local Metadata = { SchemaVersion = tostring(SchemaDefinition.Version) }
			return CommitEnvelope, UserIds or {}, Metadata
		end)
		return true
	end, { MaxRetries = self._Configuration.MaxRetries, BudgetType = "UpdateAsync" } :: any)

	if not WriteResult.Passed then
		return Errors.Fail((WriteResult :: Types.ErrorResult).Error)
	end
	if ValidationFailure then
		return Errors.Fail(Errors.New("ValidationFailed", ValidationFailure, { Key = Key }))
	end
	if TransformReturnedNil then
		return Errors.Fail(Errors.New("TransactionAborted",
			`Transform returned nil for key '{Key}'`, { Key = Key }))
	end

	return Errors.Passed(not WasConflict)
end

function Store.Transact<DataType>(
	self : StoreObject<DataType>,
	Participants : { { Key : string, Transform : (Data : DataType) -> DataType? } },
	Configuration : TransactionConfiguration?
) : TransactionResult
	local MaxRetryCount = (Configuration and Configuration.MaxRetries) or 3
	local Results : { [string] : Result<boolean> } = {}

	for Attempt = 1, MaxRetryCount do


		local ParticipantSnapshots : { [string] : { TransactionVersion : number, Data : DataType } } = {}
		for _, ParticipantEntry in Participants do
			local TargetSession = self._Sessions[ParticipantEntry.Key]
			if not TargetSession then
				return {
					Passed = false,
					Error = Errors.New("SessionNotFound",
						`No active session for '{ParticipantEntry.Key}'`, { Key = ParticipantEntry.Key }),
					Results = {},
				}
			end
			local RemoteTransactionVersion = self._LastKnownTxv[ParticipantEntry.Key] or 0
			ParticipantSnapshots[ParticipantEntry.Key] = {
				TransactionVersion = RemoteTransactionVersion,
				Data = Validation.DeepClone(TargetSession._Data),
			}
		end

		local CommittedKeys : { string } = {}
		local ConflictDetected = false
		local HardError : NexusError? = nil

		local function RollbackCommittedKeys(FailedKey : string) : { Types.TransactionRollbackStatus }
			local RollbackStatuses : { Types.TransactionRollbackStatus } = {}
			for _, CommittedKey in CommittedKeys do
				if CommittedKey == FailedKey then continue end
				local RollbackSession = self._Sessions[CommittedKey]
				local Snapshot = ParticipantSnapshots[CommittedKey]
				if not RollbackSession or not Snapshot then continue end

				-- Restore in-memory state immediately.
				RollbackSession._Data = Snapshot.Data
				RollbackSession._Dirty = true

				-- Persist the snapshot directly; bypass debounce so the rollback
				-- hits disk before this function returns, and capture failure.
				local RollbackEnvelopeResult = RollbackSession:BuildEnvelope()
				if RollbackEnvelopeResult.Passed then
					local RollbackEnvelope = (RollbackEnvelopeResult :: Types.SuccessResult<Envelope<DataType>>).Value
					if self._LockManager then
						local LockInfo = self._LockManager:ToLockInfo(CommittedKey)
						if LockInfo then RollbackEnvelope._lock = LockInfo end
					end
					local PersistResult = self:_WriteEnvelope(CommittedKey, RollbackEnvelope, RollbackSession:GetUserIds())
					if PersistResult.Passed then
						RollbackSession:MarkSaved()
						table.insert(RollbackStatuses, { Key = CommittedKey, Succeeded = true, Error = nil } :: Types.TransactionRollbackStatus)
					else
						local PersistError = (PersistResult :: Types.ErrorResult).Error
						warn(`[Nexus] Transact: rollback persist failed for '{CommittedKey}': {PersistError.Message}`)
						table.insert(RollbackStatuses, { Key = CommittedKey, Succeeded = false, Error = PersistError } :: Types.TransactionRollbackStatus)
					end
				else
					local EnvError = (RollbackEnvelopeResult :: Types.ErrorResult).Error
					table.insert(RollbackStatuses, { Key = CommittedKey, Succeeded = false, Error = EnvError } :: Types.TransactionRollbackStatus)
				end
			end
			return RollbackStatuses
		end

		for _, ParticipantEntry in Participants do
			local ParticipantSnapshot = ParticipantSnapshots[ParticipantEntry.Key]
			local ParticipantUserIds = self._Sessions[ParticipantEntry.Key]:GetUserIds()

			local CommitResult = self:_CommitParticipantCAS(
				ParticipantEntry.Key,
				ParticipantEntry.Transform,
				ParticipantSnapshot.TransactionVersion,
				ParticipantUserIds)

			if not CommitResult.Passed then
				HardError = (CommitResult :: Types.ErrorResult).Error
				local Rollbacks = RollbackCommittedKeys(ParticipantEntry.Key)
				local AnyFailed = false
				for _, R in Rollbacks do if not R.Succeeded then AnyFailed = true end end
				return {
					Passed = false,
					Error = HardError,
					Results = Results,
					Rollbacks = if #Rollbacks > 0 then Rollbacks else nil,
					PartiallyCommitted = AnyFailed,
				}
			end

			local DidCommit = (CommitResult :: Types.SuccessResult<boolean>).Value
			if not DidCommit then
				ConflictDetected = true
				RollbackCommittedKeys(ParticipantEntry.Key)
				break
			end

			table.insert(CommittedKeys, ParticipantEntry.Key)
			Results[ParticipantEntry.Key] = Errors.Passed(true)
		end

		if ConflictDetected then
			if Attempt < MaxRetryCount then
				task.wait(0.1 * (2 ^ (Attempt - 1)) + math.random() * 0.1)
				for _, ParticipantEntry in Participants do
					local CachedTxv = self._LastKnownTxv[ParticipantEntry.Key]
					local SnapshotTxv = ParticipantSnapshots[ParticipantEntry.Key].TransactionVersion
					if CachedTxv == nil or CachedTxv == SnapshotTxv then
						local PeekResult = Retry.WrapPcall("DataStoreError", function()
							return self._DataStore:GetAsync(ParticipantEntry.Key)
						end, { MaxRetries = 2, BudgetType = "GetAsync" } :: any)
						if PeekResult.Passed then
							local RawData = (PeekResult :: Types.SuccessResult<unknown>).Value
							if typeof(RawData) == "table" then
								local RemoteTable = RawData :: any
								self._LastKnownTxv[ParticipantEntry.Key] = typeof(RemoteTable._txv) == "number" and RemoteTable._txv or 0
							end
						end
					end
				end
			end
			continue
		end

		-- All participants committed. Sync local sessions to what was actually
		-- written (remote-transformed data), not a re-application of the
		-- transform against local stale state (P2-5 fix).
		for _, ParticipantEntry in Participants do
			local TargetSession = self._Sessions[ParticipantEntry.Key]
			if TargetSession then
				local TransformedData = ParticipantEntry.Transform(Validation.DeepClone(ParticipantSnapshots[ParticipantEntry.Key].Data))
				if TransformedData ~= nil then
					TargetSession._Data = TransformedData
					TargetSession._Dirty = false
				end
			end
		end

		return { Passed = true, Results = Results, Rollbacks = nil, PartiallyCommitted = false }
	end

	return {
		Passed = false,
		Error = Errors.New("TransactionConflict",
			`Transaction failed after {MaxRetryCount} CAS retries`),
		Results = Results,
		Rollbacks = nil,
		PartiallyCommitted = false,
	}
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
	local RawVersionData : unknown = nil
	local VersionKeyInfo : DataStoreKeyInfo? = nil
	local FetchResult = Retry.WrapPcall("DataStoreError", function()
		local Data, KeyInfo = (self._DataStore :: DataStore):GetVersionAsync(Key, VersionId)
		RawVersionData = Data
		VersionKeyInfo = KeyInfo
		return Data
	end, { MaxRetries = 3, BudgetType = "GetVersionAsync" } :: any)

	if not FetchResult.Passed then return Errors.Fail((FetchResult :: Types.ErrorResult).Error) end

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
	local CreatedTime = if VersionKeyInfo ~= nil then (VersionKeyInfo :: any).CreatedTime or 0 else 0
	return Errors.Passed({ Version = VersionId, CreatedTime = CreatedTime, Data = UnpackedData.Data, IsDeleted = RawVersionData == nil })
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
		local PeekResult = Retry.WrapPcall("DataStoreError", function() return self._DataStore:GetAsync(Key) end, { MaxRetries = 1, BudgetType = "GetAsync" } :: any)
		local PeekSuccess = PeekResult.Passed
		local PeekData = if PeekSuccess then (PeekResult :: Types.SuccessResult<unknown>).Value else nil
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
			if not string.match(Item.KeyName, "_shard_%d+$") then
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
		_Done = false,
		_PageCount = 0,
		_MaxPages = 1000,
	} :: PageCursor)
end

function Store.NextPage<DataType>(self : StoreObject<DataType>, Cursor : PageCursor) : Result<{ Keys : { string }, Done : boolean }>
	if Cursor._Done then return Errors.Passed({ Keys = {}, Done = true }) end
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
		if not string.match(Item.KeyName, "_shard_%d+$") then
			table.insert(Keys, Item.KeyName :: string)
		end
	end

	Cursor._Done = Pages.IsFinished or Cursor._PageCount >= Cursor._MaxPages
	return Errors.Passed({ Keys = Keys, Done = Cursor._Done :: boolean })
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

	if not WriteResult.Passed then
		self._SoftDeleted[Key] = nil
		return Errors.Fail((WriteResult :: Types.ErrorResult).Error)
	end

	self:_RecordAudit(Key, "SoftDelete", RawData, nil)
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

	local UnpackResult = SessionModule.UnpackEnvelope(Snapshot, self._Configuration.Schema)
	if not UnpackResult.Passed then
		self._Telemetry:Record("Store.Recover", os.clock() - StartTime, false)
		return Errors.Fail((UnpackResult :: Types.ErrorResult).Error)
	end

	local Data = (UnpackResult :: Types.SuccessResult<{ Data : DataType, Version : number }>).Value.Data
	self._Telemetry:Record("Store.Recover", os.clock() - StartTime, true)
	return Errors.Passed(Data)
end

function Store.IsSoftDeleted<DataType>(self : StoreObject<DataType>, Key : string) : boolean
	return self._SoftDeleted[Key] ~= nil
end

function Store.ListSoftDeleted<DataType>(self : StoreObject<DataType>) : Result<{ SoftDeleteInfo }>
	local Entries : { SoftDeleteInfo } = {}
	local Retention = self._Configuration.SoftDeleteRetention or DEFAULT_SOFT_DELETE_RETENTION
	for Key, Entry in self._SoftDeleted do
		table.insert(Entries, {
			Key = Key,
			DeletedAt = Entry.DeletedAt,
			ExpiresAt = if Retention > 0 then Entry.DeletedAt + Retention else nil,
		} :: SoftDeleteInfo)
	end
	return Errors.Passed(Entries)
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
		self._AuditDirty[Key] = nil

		local AuditKey = Key .. "_audit"
		local AuditShardCount = self._AuditShardCounts[Key] or 0
		if AuditShardCount > 0 then
			Shard.DeleteAllShards(self._DataStore, AuditKey, AuditShardCount)
		end
		pcall(function() self._DataStore:RemoveAsync(AuditKey) end)
		self._AuditShardCounts[Key] = nil
		self:_RecordAudit(Key, "PurgeSoftDeleted", nil, nil)
	end

	return RemoveResult
end

function Store.GetAuditLog<DataType>(self : StoreObject<DataType>, Key : string) : Result<{ AuditEntry }>
	return Errors.Passed(self._AuditLogs[Key] or {})
end

function Store._FlushAuditLog<DataType>(self : StoreObject<DataType>, Key : string) : ()
	if not self._Configuration.AuditPersistence then return end
	if not self._AuditDirty[Key] then return end

	local LocalEntries = self._AuditLogs[Key]
	if not LocalEntries or #LocalEntries == 0 then
		self._AuditDirty[Key] = false
		return
	end

	local AuditKey = Key .. "_audit"
	local MaxSize = self._Configuration.AuditLogSize or DEFAULT_AUDIT_LOG_SIZE
	local ShardThreshold = self._Configuration.AuditShardThreshold or DEFAULT_AUDIT_SHARD_THRESHOLD
	local MaxRetries = self._Configuration.MaxRetries or 5


	local ExistingEntries : { AuditEntry } = {}
	local OldAuditShardCount = self._AuditShardCounts[Key] or 0

	local ReadSuccess, ReadResult = pcall(function()
		return self._DataStore:GetAsync(AuditKey)
	end)

	if ReadSuccess and ReadResult ~= nil and typeof(ReadResult) == "table" then
		local RemoteTable = ReadResult :: any
		local RemoteShardCount = RemoteTable._s
		if typeof(RemoteShardCount) == "number" and RemoteShardCount > 0 then

			OldAuditShardCount = RemoteShardCount
			local ShardReadResult = Shard.ReadShards(self._DataStore, AuditKey, RemoteShardCount, MaxRetries)
			if ShardReadResult.Passed then
				local CompressedPayload = (ShardReadResult :: Types.SuccessResult<string>).Value
				local DecompressResult = Compression.DecompressToString(CompressedPayload)
				if DecompressResult.Passed then
					local JsonString = (DecompressResult :: Types.SuccessResult<string>).Value
					local ParseSuccess, ParsedEntries = pcall(HttpService.JSONDecode, HttpService, JsonString)
					if ParseSuccess and typeof(ParsedEntries) == "table" then
						ExistingEntries = ParsedEntries :: any
					end
				end
			end
		elseif RemoteTable._c == true and typeof(RemoteTable._d) == "string" then

			local DecompressResult = Compression.DecompressToString(RemoteTable._d :: string)
			if DecompressResult.Passed then
				local JsonString = (DecompressResult :: Types.SuccessResult<string>).Value
				local ParseSuccess, ParsedEntries = pcall(HttpService.JSONDecode, HttpService, JsonString)
				if ParseSuccess and typeof(ParsedEntries) == "table" then
					ExistingEntries = ParsedEntries :: any
				end
			end
		end
	end


	local MergedEntries : { AuditEntry } = {}


	for _, Entry in ExistingEntries do
		table.insert(MergedEntries, Entry)
	end


	for _, Entry in LocalEntries do
		table.insert(MergedEntries, Entry)
	end


	while #MergedEntries > MaxSize do
		table.remove(MergedEntries, 1)
	end


	local EncodeSuccess, JsonPayload = pcall(HttpService.JSONEncode, HttpService, MergedEntries)
	if not EncodeSuccess then
		warn(`[Nexus] _FlushAuditLog: JSON encode failed for '{AuditKey}': {JsonPayload}`)
		return
	end

	local CompressResult = Compression.CompressString(JsonPayload :: string)
	if not CompressResult.Passed then
		warn(`[Nexus] _FlushAuditLog: compress failed for '{AuditKey}'`)
		return
	end

	local CompressedPayload = (CompressResult :: Types.SuccessResult<string>).Value


	local NeedsSharding = #CompressedPayload > ShardThreshold and ShardThreshold > 0

	if NeedsSharding then
		local Chunks = Shard.Split(CompressedPayload, ShardThreshold)
		local NewShardCount = #Chunks

		local ShardWriteResult = Shard.WriteShards(self._DataStore, AuditKey, Chunks, MaxRetries)
		if not ShardWriteResult.Passed then
			warn(`[Nexus] _FlushAuditLog: shard write failed for '{AuditKey}' — dirty flag preserved for retry`)
			return  -- leave _AuditDirty[Key] = true so the next flush retries
		end

		local ManifestSuccess, ManifestError = pcall(function()
			self._DataStore:SetAsync(AuditKey, { _s = NewShardCount })
		end)
		if not ManifestSuccess then
			warn(`[Nexus] _FlushAuditLog: manifest SetAsync failed for '{AuditKey}': {ManifestError}`)
			return  -- shards written but manifest failed; dirty flag stays for retry
		end

		if OldAuditShardCount > NewShardCount then
			Shard.CleanupOrphanedShards(self._DataStore, AuditKey, OldAuditShardCount, NewShardCount)
		end

		self._AuditShardCounts[Key] = NewShardCount
		self:_DebugLog("AuditFlush.Sharded", Key)
	else
		local DirectSuccess, DirectError = pcall(function()
			self._DataStore:SetAsync(AuditKey, {
				_c = true,
				_d = CompressedPayload,
			})
		end)
		if not DirectSuccess then
			warn(`[Nexus] _FlushAuditLog: SetAsync failed for '{AuditKey}': {DirectError} — dirty flag preserved for retry`)
			return  -- leave _AuditDirty[Key] = true so the next flush retries
		end

		if OldAuditShardCount > 0 then
			Shard.DeleteAllShards(self._DataStore, AuditKey, OldAuditShardCount)
		end

		self._AuditShardCounts[Key] = 0
		self:_DebugLog("AuditFlush.Direct", Key)
	end

	-- Only clear dirty after a confirmed successful write.
	self._AuditDirty[Key] = false
end

function Store.GetAuditLogRemote<DataType>(self : StoreObject<DataType>, Key : string) : Result<{ AuditEntry }>
	local AuditKey = Key .. "_audit"

	local FetchResult = Retry.WrapPcall("DataStoreError", function()
		return self._DataStore:GetAsync(AuditKey)
	end, { MaxRetries = self._Configuration.MaxRetries, BudgetType = "GetAsync" } :: any)

	if not FetchResult.Passed then
		return Errors.Fail((FetchResult :: Types.ErrorResult).Error)
	end

	local RawData = (FetchResult :: Types.SuccessResult<unknown>).Value
	if RawData == nil then
		return Errors.Passed({})
	end

	if typeof(RawData) ~= "table" then
		return Errors.Fail(Errors.New("Internal",
			`Audit log for '{Key}' has unexpected type '{typeof(RawData)}'`, { Key = Key }))
	end

	local RemoteTable = RawData :: any
	local CompressedPayload : string? = nil


	local RemoteShardCount = RemoteTable._s
	if typeof(RemoteShardCount) == "number" and RemoteShardCount > 0 then
		local ShardReadResult = Shard.ReadShards(
			self._DataStore, AuditKey, RemoteShardCount, self._Configuration.MaxRetries)
		if not ShardReadResult.Passed then
			return Errors.Fail((ShardReadResult :: Types.ErrorResult).Error)
		end
		CompressedPayload = (ShardReadResult :: Types.SuccessResult<string>).Value
		self._AuditShardCounts[Key] = RemoteShardCount
	elseif RemoteTable._c == true and typeof(RemoteTable._d) == "string" then
		CompressedPayload = RemoteTable._d :: string
		self._AuditShardCounts[Key] = 0
	else
		return Errors.Passed({})
	end


	local DecompressResult = Compression.DecompressToString(CompressedPayload :: string)
	if not DecompressResult.Passed then
		return Errors.Fail((DecompressResult :: Types.ErrorResult).Error)
	end

	local JsonString = (DecompressResult :: Types.SuccessResult<string>).Value
	local ParseSuccess, ParsedEntries = pcall(HttpService.JSONDecode, HttpService, JsonString)
	if not ParseSuccess then
		return Errors.Fail(Errors.New("Internal",
			`Failed to parse audit log JSON for '{Key}': {ParsedEntries}`, { Key = Key }))
	end

	return Errors.Passed(ParsedEntries :: { AuditEntry })
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
		local PeekResult = Retry.WrapPcall("DataStoreError", function() return self._DataStore:GetAsync(Key) end, { MaxRetries = 1, BudgetType = "GetAsync" } :: any)
		local PeekSuccess = PeekResult.Passed
		local PeekData = if PeekSuccess then (PeekResult :: Types.SuccessResult<unknown>).Value else nil
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
	self._AuditDirty[Key] = nil
	self._SoftDeleted[Key] = nil


	local AuditKey = Key .. "_audit"
	local AuditShardCount = self._AuditShardCounts[Key] or 0
	if AuditShardCount > 0 then
		Shard.DeleteAllShards(self._DataStore, AuditKey, AuditShardCount)
	end
	pcall(function() self._DataStore:RemoveAsync(AuditKey) end)
	self._AuditShardCounts[Key] = nil

	return Retry.WrapPcall("DataStoreError", function()
		self._DataStore:RemoveAsync(Key)
		return true
	end, { MaxRetries = 3, BudgetType = "SetIncrementAsync" } :: any)
end

function Store.Destroy<DataType>(self : StoreObject<DataType>) : ()
	self._ShuttingDown = true
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
