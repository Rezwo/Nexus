--!strict

local Types = {}

export type Result<Value> = SuccessResult<Value> | ErrorResult

export type SuccessResult<Value> = {
	Passed : true,
	Value : Value,
}

export type ErrorResult = {
	Passed : false,
	Error : NexusError,
}

export type NexusError = {
	Kind : ErrorKind,
	Message : string,
	Code : number?,
	Key : string?,
	Retries : number?,
	Cause : string?,
}

export type ErrorKind =
	"Throttled"
| "DataStoreError"
| "MemoryStoreError"
| "SessionLocked"
| "SessionNotFound"
| "SessionClosed"
| "ValidationFailed"
| "MigrationFailed"
| "TransactionConflict"
| "TransactionAborted"
| "TransactionTimeout"
| "CompressionError"
| "SchemaError"
| "BudgetExhausted"
| "KeyNotFound"
| "StrictModeViolation"
| "Timeout"
| "Internal"

export type Connection = {
	Disconnect : (self : Connection) -> (),
	Connected : boolean,
}

export type Signal<T...> = {
	Connect : (self : Signal<T...>, Callback : (T...) -> ()) -> Connection,
	Once : (self : Signal<T...>, Callback : (T...) -> ()) -> Connection,
	Fire : (self : Signal<T...>, T...) -> (),
	Wait : (self : Signal<T...>) -> T...,
	Destroy : (self : Signal<T...>) -> (),
}

export type MigrationStep = {
	Version : number,
	Up : (Data : { [string] : unknown }) -> { [string] : unknown },
}

export type SchemaDefinition<DataType> = {
	Version : number,
	Default : DataType,
	Migrations : { MigrationStep }?,
	Validate : ((Data : DataType) -> (boolean, string?))?,
}

export type Envelope<DataType> = {
	_v : number,
	_c : boolean?,
	_d : DataType | string,
	_lock : LockInfo?,
	_s : number?,
	_deleted : boolean?,
	_deletedAt : number?,
}

export type LockInfo = {
	jobId : string,
	renewedAt : number,
	lockVersion : number,
}

export type SessionStatus = "Loading" | "Active" | "Saving" | "Closing" | "Closed" | "Error"

export type SessionInfo = {
	Key : string,
	Status : SessionStatus,
	LastSaveTime : number,
	LockVersion : number,
	Dirty : boolean,
	SchemaVersion : number,
	SaveCount : number,
	SessionAge : number,
	PendingActionCount : number,
}

export type StoreConfiguration<DataType> = {
	Name : string,
	Scope : string?,
	Schema : SchemaDefinition<DataType>,
	LockMode : LockMode?,
	AutoSaveInterval : number?,
	Compress : boolean?,
	CompressionThreshold : number?,
	CompressionLevel : number?,
	MaxRetries : number?,
	Strict : boolean?,
	GetUserIds : ((Key : string) -> { number })?,
	ShardThreshold : number?,
	ValidateOnLoad : boolean?,
	DebounceInterval : number?,
	SoftDeleteRetention : number?,
	AuditLog : boolean?,
	AuditLogSize : number?,
	Debug : boolean?,
	IdleTimeout : number?,
	IdleCheckInterval : number?,
}

export type LockMode =
	"Session"
| "Optimistic"
| "None"

export type TransactionParticipant<DataType> = {
	Store : string,
	Key : string,
	Transform : (Data : DataType) -> DataType?,
}

export type TransactionConfiguration = {
	Timeout : number?,
	MaxRetries : number?,
}

export type TransactionResult = {
	Passed : boolean,
	Error : NexusError?,
	Results : { [string] : Result<boolean> },
}

export type VersionInfo = {
	Version : string,
	CreatedTime : number,
	IsDeleted : boolean,
}

export type VersionEntry<DataType> = {
	Version : string,
	CreatedTime : number,
	Data : DataType,
	IsDeleted : boolean,
}

export type BudgetType =
	"GetAsync"
| "SetIncrementAsync"
| "UpdateAsync"
| "GetSortedAsync"
| "SetIncrementSortedAsync"
| "OnUpdate"
| "GetVersionAsync"
| "ListAsync"

export type RetryConfiguration = {
	MaxRetries : number?,
	BudgetType : BudgetType?,
	OnRetry : ((Attempt : number, Delay : number, LastError : NexusError) -> ())?,
}

export type StoreCallbacks<DataType> = {
	OnLoad : ((Key : string, Data : DataType) -> ())?,
	OnSave : ((Key : string, Data : DataType) -> ())?,
	OnRelease : ((Key : string) -> ())?,
	OnError : ((Key : string, Error : NexusError) -> ())?,
	OnConflict : ((Key : string, LocalData : DataType, RemoteData : DataType) -> DataType)?,
	OnMigration : ((Key : string, FromVersion : number, ToVersion : number) -> ())?,
	BeforeSave : ((Key : string, Data : DataType) -> DataType?)?,
	AfterLoad : ((Key : string, Data : DataType) -> DataType?)?,
}

export type BatchResult<DataType> = {
	Succeeded : { [string] : DataType },
	Failed : { [string] : NexusError },
}

export type PageCursor = {
	_Pages : any,
	_Done : boolean,
	_PageCount : number,
	_MaxPages : number,
}

export type AuditEntry = {
	Timestamp : number,
	Operation : string,
	Before : unknown?,
	After : unknown?,
	Source : string,
	SessionId : string,
}

export type MiddlewarePhase = "load" | "save"

export type DebugContext = {
	Store : string,
	Key : string?,
	Operation : string,
	Elapsed : number?,
}

export type SoftDeleteInfo = {
	Key : string,
	DeletedAt : number,
	ExpiresAt : number?,
}

return Types
