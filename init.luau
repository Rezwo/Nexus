--!strict

local Types = require(script.Types)
local Errors = require(script.Errors)
local Store = require(script.Store)
local Validation = require(script.Validation)
local Circuit = require(script.Circuit)
local Lifecycle = require(script.Lifecycle)
local Semaphore = require(script.Semaphore)
local Leaderboard = require(script.Leaderboard)
local PlayerStoreModule = require(script.PlayerStore)
local Telemetry = require(script.Telemetry)
local InboxModule = require(script.Inbox)
local Budget = require(script.Budget)
local MemoryBudget = require(script.MemoryBudget)
local Transaction = require(script.Transaction)
local MockDataStoreModule = require(script.MockDataStore)

export type Result<Value> = Types.Result<Value>
export type SuccessResult<Value> = Types.SuccessResult<Value>
export type ErrorResult = Types.ErrorResult
export type NexusError = Types.NexusError
export type ErrorKind = Types.ErrorKind
export type StoreConfiguration<DataType> = Types.StoreConfiguration<DataType>
export type SchemaDefinition<DataType> = Types.SchemaDefinition<DataType>
export type MigrationStep = Types.MigrationStep
export type SessionInfo = Types.SessionInfo
export type VersionInfo = Types.VersionInfo
export type VersionEntry<DataType> = Types.VersionEntry<DataType>
export type TransactionConfiguration = Types.TransactionConfiguration
export type TransactionResult = Types.TransactionResult
export type StoreCallbacks<DataType> = Types.StoreCallbacks<DataType>
export type LockMode = Types.LockMode
export type LockInfo = Types.LockInfo
export type Connection = Types.Connection
export type CircuitState = Circuit.CircuitState
export type Store<DataType> = Store.StoreObject<DataType>
export type ListVersionsOptions = Store.ListVersionsOptions
export type CircuitInfo = Store.CircuitInfo
export type SemaphoreObject = Semaphore.SemaphoreObject
export type Leaderboard = Leaderboard.LeaderboardObject
export type LeaderboardEntry = Leaderboard.LeaderboardEntry
export type LeaderboardConfiguration = Leaderboard.LeaderboardConfiguration
export type PlayerStore<DataType> = PlayerStoreModule.PlayerStoreObject<DataType>
export type PlayerStoreConfiguration<DataType> = PlayerStoreModule.PlayerStoreConfiguration<DataType>
export type OperationStats = Telemetry.OperationStats
export type TelemetryScope = Telemetry.TelemetryScope
export type InboxManager = InboxModule.InboxManager
export type InboxItem = InboxModule.InboxItem
export type InboxConfiguration = InboxModule.InboxConfiguration
export type BatchResult<DataType> = Types.BatchResult<DataType>
export type PageCursor = Types.PageCursor
export type AuditEntry = Types.AuditEntry
export type MiddlewarePhase = Types.MiddlewarePhase
export type DebugContext = Types.DebugContext
export type SoftDeleteInfo = Types.SoftDeleteInfo
export type MockStore = MockDataStoreModule.MockStore
export type MockStoreOptions = MockDataStoreModule.MockStoreOptions
export type TransactionRollbackStatus = Types.TransactionRollbackStatus

local Nexus = {}

function Nexus.CreateStore<DataType>(
	Configuration : StoreConfiguration<DataType>
) : Store.StoreObject<DataType>
	return Store.New(Configuration)
end

Nexus.CreateError = Errors.New
Nexus.Passed = Errors.Passed
Nexus.Err = Errors.Fail
Nexus.FormatError = Errors.Format

Nexus.IsJsonSafe = Validation.IsJsonSafe
Nexus.DeepClone = Validation.DeepClone
Nexus.DeepEqual = Validation.DeepEqual

Nexus.Lifecycle = Lifecycle
Nexus.Telemetry = Telemetry
Nexus.Budget = Budget
Nexus.MemoryBudget = MemoryBudget
Nexus.MockDataStore = MockDataStoreModule

Nexus.CreateSemaphore = Semaphore.New

function Nexus.GetWriteSemaphoreInfo() : {
	MaxConcurrent : number,
	ActiveCount : number,
	QueueDepth : number,
	}
	return Semaphore.Global:GetInfo()
end

function Nexus.SetRateLimit(
	RequestType : Enum.DataStoreRequestType,
	BaseLimit : number,
	PerPlayerLimit : number
) : ()
	local DataStoreService = game:GetService("DataStoreService");
	(DataStoreService :: any):SetRateLimitForRequestType(RequestType, BaseLimit, PerPlayerLimit)
end

function Nexus.SetDebugMode(Enabled : boolean) : ()
	Store.SetGlobalDebugMode(Enabled)
end

function Nexus.SetDebugHandler(Handler : ((Types.DebugContext) -> ())?) : ()
	Store.SetGlobalDebugHandler(Handler)
end

function Nexus.Transact(
	Participants : { { Store : Store.StoreObject<any>, Key : string, Transform : (Data : any) -> any? } },
	Configuration : TransactionConfiguration?
) : TransactionResult
	-- Pre-validate: every participant must have an active session so rollback
	-- snapshots can be captured. Fail early rather than discovering a missing
	-- session mid-execution with some keys already committed.
	for _, Entry in Participants do
		local TargetSession = (Entry.Store :: any)._Sessions[Entry.Key]
		if not TargetSession then
			local MissingError = Errors.New("SessionNotFound",
				`No active session for key '{Entry.Key}' in cross-store transaction`,
				{ Key = Entry.Key }
			)
			return {
				Passed = false,
				Error = MissingError,
				Results = {},
				Rollbacks = nil,
				PartiallyCommitted = false,
			}
		end
	end

	local TransactionInstance = Transaction.New(Configuration)

	for _, Entry in Participants do
		TransactionInstance:Add(Entry.Store, Entry.Key, Entry.Transform :: any)
	end

	local AllParticipants = TransactionInstance:GetParticipants()
	for Index, ParticipantData in AllParticipants do
		local TypedStore = ParticipantData.StoreReference :: Store.StoreObject<any>
		local TargetSession = TypedStore._Sessions[ParticipantData.Key]
		if TargetSession then
			TransactionInstance:SetSnapshot(Index, Validation.DeepClone(TargetSession._Data))
		end
	end

	return TransactionInstance:ExecuteOptimistic(
		function(StoreReference : unknown, Key : string, Transform : (Data : unknown) -> unknown?) : Types.Result<unknown>
			local TypedStore = StoreReference :: Store.StoreObject<any>
			local TransformReturnedNil = false
			local WrappedTransform = function(Data : any) : any?
				local TransformResult = (Transform :: any)(Data)
				if TransformResult == nil then
					TransformReturnedNil = true
				end
				return TransformResult
			end
			local UpdateResult = TypedStore:Update(Key, WrappedTransform)
			if TransformReturnedNil then
				return Errors.Fail(Errors.New("TransactionAborted",
					`Transform returned nil for key '{Key}'`, { Key = Key }))
			end
			return UpdateResult :: any
		end,
		function(StoreReference : unknown, Key : string, Snapshot : unknown) : Types.Result<boolean>
			local TypedStore = StoreReference :: Store.StoreObject<any>
			local TargetSession = TypedStore._Sessions[Key]
			if TargetSession then
				TargetSession._Data = Snapshot
				TargetSession._Dirty = true
				-- Bypass debounce: a crash during the debounce window would persist
				-- the bad (pre-rollback) state on next load.
				local DebounceThread = (TypedStore :: any)._DebounceTimers[Key]
				if DebounceThread then
					task.cancel(DebounceThread)
					;(TypedStore :: any)._DebounceTimers[Key] = nil
				end
				local PersistResult = (TypedStore :: any):_PerformDebouncedSave(Key)
				if not PersistResult.Passed then
					warn(`[Nexus] Cross-store transaction: rollback persist failed for '{Key}'`)
				end
			end
			return Errors.Passed(true)
		end)
end

local function CheckErrorKind(OperationResult : Types.Result<any>, Kind : Types.ErrorKind) : boolean
	if OperationResult.Passed then return false end
	return (OperationResult :: Types.ErrorResult).Error.Kind == Kind
end

function Nexus.IsSessionLocked(OperationResult : Types.Result<any>) : boolean
	return CheckErrorKind(OperationResult, "SessionLocked")
end

function Nexus.IsThrottled(OperationResult : Types.Result<any>) : boolean
	return CheckErrorKind(OperationResult, "Throttled")
end

function Nexus.IsDataStoreError(OperationResult : Types.Result<any>) : boolean
	return CheckErrorKind(OperationResult, "DataStoreError")
end

function Nexus.IsValidationError(OperationResult : Types.Result<any>) : boolean
	return CheckErrorKind(OperationResult, "ValidationFailed")
end

function Nexus.IsBudgetExhausted(OperationResult : Types.Result<any>) : boolean
	return CheckErrorKind(OperationResult, "BudgetExhausted")
end

function Nexus.IsSessionNotFound(OperationResult : Types.Result<any>) : boolean
	return CheckErrorKind(OperationResult, "SessionNotFound")
end

function Nexus.CreateLeaderboard(
	Configuration : LeaderboardConfiguration
) : Leaderboard.LeaderboardObject
	return Leaderboard.New(Configuration)
end

function Nexus.CreatePlayerStore<DataType>(
	Configuration : PlayerStoreConfiguration<DataType>
) : PlayerStoreModule.PlayerStoreObject<DataType>
	return PlayerStoreModule.New(Configuration)
end

function Nexus.CreateInbox(
	Configuration : InboxConfiguration
) : InboxModule.InboxManager
	return InboxModule.New(Configuration)
end

Nexus.VERSION = "4.0.0"

Nexus.LockMode = {
	Session = "Session" :: Types.LockMode,
	Optimistic = "Optimistic" :: Types.LockMode,
	None = "None" :: Types.LockMode,
}

return Nexus
