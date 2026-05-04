# Nexus v4.0.0 Update Draft

Suggested title:

**Nexus v4.0.0 - Safer DataStore Saves, Audit Logs, Soft Deletes, Middleware, and Better Testing**

Hey developers,

Nexus has been updated from **v3.5.0** to **v4.0.0**. This release focuses on making DataStore workflows safer under real server conditions: concurrent writes, failed saves, shutdowns, player load spikes, rollback paths, and debugging difficult production issues.

This update is especially useful if your experience depends on reliable player data, cross-store transactions, auditability, or safer testing without burning live DataStore quota.

## Highlights

### Safer save and rollback behavior

- Added debounced saves so rapid updates can be coalesced before writing.
- Improved close/shutdown saves with retry handling.
- Fixed rollback behavior so rollback persistence bypasses debounce and is written immediately.
- Added stronger optimistic save behavior that replays pending actions against the latest remote value.
- Added transaction version tracking so optimistic writes can better detect conflicts.
- Added payload size guards before committing oversized DataStore entries.

### Soft deletes and recovery tools

Stores now support soft delete workflows:

```luau
Store:SoftDelete(Key)
Store:Recover(Key)
Store:IsSoftDeleted(Key)
Store:ListSoftDeleted()
Store:PurgeSoftDeleted(Key)
```

This makes it easier to temporarily hide or recover records before permanently purging them.

### Persistent audit logs

Nexus can now record audit entries for store operations and optionally persist them:

```luau
local Store = Nexus.CreateStore({
	Name = "PlayerData",
	Schema = PlayerSchema,
	AuditLog = true,
	AuditPersistence = true,
})

local LogResult = Store:GetAuditLog("Player_123")
local RemoteLogResult = Store:GetAuditLogRemote("Player_123")
```

Audit persistence now uses safer `UpdateAsync` merging for non-sharded logs to avoid overwriting entries written by another server. Larger audit logs can be compressed and sharded.

### Middleware and load/save callbacks

You can now add middleware around load and save flows:

```luau
Store:Use("load", function(Key, Data, Next)
	print("Loading", Key)
	return Next(Data)
end)

Store:Use("save", function(Key, Data, Next)
	Data.LastSavedAt = os.time()
	return Next(Data)
end)
```

New callback hooks were also added:

```luau
BeforeSave = function(Key, Data)
	return Data
end,

AfterLoad = function(Key, Data)
	return Data
end,
```

### Better observability

This update adds store-level telemetry scopes and debug helpers:

```luau
Nexus.SetDebugMode(true)
Nexus.SetDebugHandler(function(Context)
	print(Context.Store, Context.Operation, Context.Key, Context.Elapsed)
end)

local Telemetry = Store:GetTelemetry()
```

New error helper functions make result handling cleaner:

```luau
Nexus.IsSessionLocked(Result)
Nexus.IsThrottled(Result)
Nexus.IsDataStoreError(Result)
Nexus.IsValidationError(Result)
Nexus.IsBudgetExhausted(Result)
Nexus.IsSessionNotFound(Result)
```

### PlayerStore reliability improvements

- Added `MaxConcurrentLoads` to limit how many player loads run at once.
- Improved behavior when a player leaves while their data is still loading.
- Improved gift/inbox recovery so failed gift processing re-queues items instead of silently losing them.
- Close failures are now recorded with telemetry and warnings.

### MemoryStore and locking hardening

Nexus now includes a `MemoryBudget` module for tracking MemoryStore pressure, temporary backoff, queue depth, and recovery state.

Locking was also hardened with:

- Heartbeat generation guards to prevent stale heartbeat threads from writing after release.
- Clock drift tolerance before stealing leases.
- Heartbeat watchdog warnings for slow MemoryStore calls.
- Safer handling of malformed lease/envelope data.
- Better cleanup when releasing all locks.

### Offline testing and mock DataStores

Nexus now exposes `Nexus.MockDataStore`, an in-memory DataStore replacement for Studio tests and local test harnesses.

It supports the DataStore APIs Nexus depends on, including:

- `GetAsync`
- `SetAsync`
- `UpdateAsync`
- `RemoveAsync`
- `IncrementAsync`
- `ListKeysAsync`
- `ListVersionsAsync`
- `GetVersionAsync`
- `GetSortedAsync`

It also includes fault and throttle simulation, making it easier to test retry paths, failure paths, and edge cases without touching live DataStore quota.

New test suites were added for:

- Boundary cases
- Concurrency behavior
- Failure/retry behavior
- Deep regression coverage
- Version history and rollback behavior
- Audit log persistence

Unit tests were also run against the updated code paths to help verify the new reliability fixes and regression coverage.

## New API Surface

Top-level additions:

```luau
Nexus.VERSION -- "4.0.0"
Nexus.MemoryBudget
Nexus.MockDataStore
Nexus.Transact(...)
Nexus.SetDebugMode(...)
Nexus.SetDebugHandler(...)
```

Store additions:

```luau
Store:Peek(Key)
Store:LoadMany(Keys)
Store:QueueAction(Key, Transform)
Store:ListKeysPage(Prefix, PageSize)
Store:NextPage(Cursor)
Store:SoftDelete(Key)
Store:Recover(Key)
Store:IsSoftDeleted(Key)
Store:ListSoftDeleted()
Store:PurgeSoftDeleted(Key)
Store:GetAuditLog(Key)
Store:GetAuditLogRemote(Key)
Store:GetTelemetry()
Store:Use("load", Handler)
Store:Use("save", Handler)
```

Configuration additions:

```luau
ValidateOnLoad
DebounceInterval
SoftDeleteRetention
AuditLog
AuditLogSize
AuditPersistence
AuditShardThreshold
Debug
IdleTimeout
IdleCheckInterval
WriteCooldown
ReadOnly
CloseSaveMaxRetries
```

PlayerStore addition:

```luau
MaxConcurrentLoads
```

## Migration Notes

- `Nexus.VERSION` is now `4.0.0`.
- If your schema version is above `1`, Nexus now validates that migrations cover every version from `2` to your target schema version. Missing or duplicate migration steps will assert earlier instead of failing silently later.
- `ReadOnly = true` stores should use `Store:Peek(Key)` for non-locking reads. `Store:Load(Key)` is not allowed for read-only stores because it acquires a session lock.
- If you rely on rollback behavior, note that rollback now persists immediately instead of waiting inside the debounce window.
- `GetVersion` and `Rollback` are intended for normal DataStore versions. Sharded historical versions cannot be safely read or rolled back as a single version entry.
- If you install directly from GitHub, use the current `origin/main` version so you include the latest rollback, audit-log race, and PlayerStore concurrency fixes.

## Why This Update Matters

The main goal of v4.0.0 is reliability. A lot of the work in this release is about preventing rare but painful data issues:

- losing rollback data during a debounce window,
- overwriting audit logs from another server,
- holding player sessions after the player leaves,
- overloading player load flows during spikes,
- letting malformed lock data block a key,
- or shipping changes without a reliable offline test harness.

If you are already using Nexus, this update is worth reviewing before your next production deployment.

## Source Comparison

This draft was prepared from the repository history:

- Previous version marker: `v3.5.0` at commit `8a23c13`
- Current remote version: `v4.0.0` on `origin/main` at commit `efc97fd`
- No Git tags or GitHub release tags were present in the local repository history
