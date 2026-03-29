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
export type InboxManager = InboxModule.InboxManager
export type InboxItem = InboxModule.InboxItem
export type InboxConfiguration = InboxModule.InboxConfiguration

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

Nexus.CreateSemaphore = Semaphore.New

function Nexus.GetWriteSemaphoreInfo() : {
	MaxConcurrent : number,
	ActiveCount : number,
	QueueDepth : number,
}
	return Semaphore.Global:GetInfo()
end

Nexus.Budget = Budget

function Nexus.SetRateLimit(
	RequestType : Enum.DataStoreRequestType,
	BaseLimit : number,
	PerPlayerLimit : number
) : ()
	local DataStoreService = game:GetService("DataStoreService");
	(DataStoreService :: any):SetRateLimitForRequestType(RequestType, BaseLimit, PerPlayerLimit)
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

Nexus.VERSION = "3.5.0"

Nexus.LockMode = {
	Session = "Session" :: Types.LockMode,
	Optimistic = "Optimistic" :: Types.LockMode,
	None = "None" :: Types.LockMode,
}

return Nexus
