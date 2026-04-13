--!strict
--[[
	Nexus Fixes Test Script
	========================
	Run this as a Script in ServerScriptService in Roblox Studio.
	Requires the Nexus module to be accessible at the path below.
	
	Tests:
	  1. SaveOptimistic sharded fallback
	  2. Transaction CAS with _txv tracking
	  3. MemoryBudget per-resource isolation
]]

local Nexus = require(game.ReplicatedStorage.Nexus)

local PASS = "✅"
local FAIL = "❌"
local TestCount = 0
local PassCount = 0
local FailCount = 0

local function Assert(Condition : boolean, TestName : string, Detail : string?)
	TestCount += 1
	if Condition then
		PassCount += 1
		print(`{PASS} {TestName}`)
	else
		FailCount += 1
		warn(`{FAIL} {TestName}{if Detail then ": " .. Detail else ""}`)
	end
end

local function Section(Title : string)
	print(`\n{string.rep("=", 60)}`)
	print(`  {Title}`)
	print(`{string.rep("=", 60)}`)
end

-- ── Schema shared across all tests ──────────────────────────────────────────

local TestSchema = {
	Version = 1,
	Default = {
		Coins = 0,
		Gems = 0,
		Inventory = {},
	},
}

-- ══════════════════════════════════════════════════════════════════════════════
-- TEST 1: MemoryBudget Per-Resource Isolation
-- ══════════════════════════════════════════════════════════════════════════════

Section("Fix 3: MemoryBudget Per-Resource Isolation")

local MemoryBudget = Nexus.MemoryBudget

-- Reset global state for clean testing
MemoryBudget.Reset()

-- 1a. Independent failure tracking
do
	MemoryBudget.RecordFailure("InventoryStore")
	MemoryBudget.RecordFailure("InventoryStore")

	local InventoryStatus = MemoryBudget.GetStatus("InventoryStore")
	local CurrencyStatus = MemoryBudget.GetStatus("CurrencyStore")

	Assert(InventoryStatus.RecentFailureCount == 2,
		"Isolated failure count: InventoryStore has 2 failures",
		`Got {InventoryStatus.RecentFailureCount}`)

	Assert(CurrencyStatus.RecentFailureCount == 0,
		"Isolated failure count: CurrencyStore has 0 failures",
		`Got {CurrencyStatus.RecentFailureCount}`)
end

-- 1b. Backoff isolation — hammer one resource to trigger backoff
do
	MemoryBudget.Reset()

	for Iteration = 1, 6 do
		MemoryBudget.RecordFailure("NoisyStore")
	end

	local NoisyCanAttempt = MemoryBudget.ShouldAttempt("NoisyStore")
	local CleanCanAttempt = MemoryBudget.ShouldAttempt("CleanStore")

	Assert(not NoisyCanAttempt,
		"Backoff triggered: NoisyStore is in backoff after 6 failures")

	Assert(CleanCanAttempt,
		"Backoff isolated: CleanStore is NOT in backoff despite NoisyStore being throttled")
end

-- 1c. Success erodes failure window
do
	MemoryBudget.Reset()

	MemoryBudget.RecordFailure("ErodingStore")
	MemoryBudget.RecordFailure("ErodingStore")
	MemoryBudget.RecordFailure("ErodingStore")

	local BeforeSuccess = MemoryBudget.GetStatus("ErodingStore")
	Assert(BeforeSuccess.RecentFailureCount == 3,
		"Before RecordSuccess: 3 failures",
		`Got {BeforeSuccess.RecentFailureCount}`)

	MemoryBudget.RecordSuccess("ErodingStore")

	local AfterSuccess = MemoryBudget.GetStatus("ErodingStore")
	Assert(AfterSuccess.RecentFailureCount == 2,
		"After RecordSuccess: 2 failures (one eroded)",
		`Got {AfterSuccess.RecentFailureCount}`)
end

-- 1d. HasCapacity mirrors ShouldAttempt
do
	MemoryBudget.Reset()

	Assert(MemoryBudget.HasCapacity("FreshStore") == true,
		"HasCapacity returns true for fresh resource")

	for Iteration = 1, 6 do
		MemoryBudget.RecordFailure("FullStore")
	end

	Assert(MemoryBudget.HasCapacity("FullStore") == false,
		"HasCapacity returns false for resource in backoff")
end

-- 1e. Per-resource reset
do
	MemoryBudget.Reset()

	MemoryBudget.RecordFailure("StoreA")
	MemoryBudget.RecordFailure("StoreA")
	MemoryBudget.RecordFailure("StoreB")

	MemoryBudget.Reset("StoreA")

	local StatusA = MemoryBudget.GetStatus("StoreA")
	local StatusB = MemoryBudget.GetStatus("StoreB")

	Assert(StatusA.RecentFailureCount == 0,
		"Per-resource reset: StoreA cleared",
		`Got {StatusA.RecentFailureCount}`)

	Assert(StatusB.RecentFailureCount == 1,
		"Per-resource reset: StoreB untouched",
		`Got {StatusB.RecentFailureCount}`)
end

-- ══════════════════════════════════════════════════════════════════════════════
-- TEST 2: SaveOptimistic Sharded Fallback
-- ══════════════════════════════════════════════════════════════════════════════

Section("Fix 1: SaveOptimistic Sharded Fallback")

do
	local CoinStore = Nexus.CreateStore({
		Name = "NexusTest_SaveOptimistic",
		Schema = TestSchema,
		LockMode = "None",
	})

	local LoadResult = CoinStore:Load("ShardTestKey")
	Assert(LoadResult.Passed == true,
		"SaveOptimistic: Load succeeded")

	if LoadResult.Passed then
		-- Update session data so it's dirty
		local UpdateResult = CoinStore:Update("ShardTestKey", function(Data)
			Data.Coins = 100
			return Data
		end)
		Assert(UpdateResult.Passed == true,
			"SaveOptimistic: Update succeeded")

		-- Queue an action so SaveOptimistic has pending actions
		local QueueResult = CoinStore:QueueAction("ShardTestKey", function(Data)
			Data.Coins += 50
			return Data
		end)
		Assert(QueueResult.Passed == true,
			"SaveOptimistic: QueueAction succeeded")

		-- Artificially mark this key as sharded
		-- This simulates the scenario where data grew large enough to shard
		;(CoinStore :: any)._ShardCounts["ShardTestKey"] = 2

		-- SaveOptimistic should NOT error — it should gracefully fallback
		local OptimisticResult = CoinStore:SaveOptimistic("ShardTestKey")
		Assert(OptimisticResult.Passed == true,
			"SaveOptimistic: Sharded key falls back gracefully instead of erroring")

		-- Clean up
		;(CoinStore :: any)._ShardCounts["ShardTestKey"] = 0
		CoinStore:Close("ShardTestKey")
	end

	CoinStore:Destroy()
end

-- ══════════════════════════════════════════════════════════════════════════════
-- TEST 3: Transaction CAS with _txv Tracking
-- ══════════════════════════════════════════════════════════════════════════════

Section("Fix 2: Transaction CAS via _txv")

-- 3a. _txv is populated on Load and incremented on Save
do
	local TransactionStore = Nexus.CreateStore({
		Name = "NexusTest_TransactionCAS",
		Schema = TestSchema,
		LockMode = "None",
	})

	local LoadResult = TransactionStore:Load("TxvKey1")
	Assert(LoadResult.Passed == true,
		"Transaction CAS: Load TxvKey1 succeeded")

	if LoadResult.Passed then
		local InitialTxv = (TransactionStore :: any)._LastKnownTxv["TxvKey1"]
		Assert(InitialTxv ~= nil,
			"Transaction CAS: _LastKnownTxv populated after Load",
			`Got {tostring(InitialTxv)}`)

		-- Update and save to increment _txv
		TransactionStore:Update("TxvKey1", function(Data)
			Data.Coins = 999
			return Data
		end)

		local SaveResult = TransactionStore:Save("TxvKey1")
		Assert(SaveResult.Passed == true,
			"Transaction CAS: Save succeeded")

		if SaveResult.Passed then
			local PostSaveTxv = (TransactionStore :: any)._LastKnownTxv["TxvKey1"]
			Assert(typeof(PostSaveTxv) == "number" and PostSaveTxv > (InitialTxv or 0),
				"Transaction CAS: _txv incremented after Save",
				`Before={InitialTxv}, After={PostSaveTxv}`)
		end

		TransactionStore:Close("TxvKey1")
	end

	TransactionStore:Destroy()
end

-- 3b. _LastKnownTxv is cleaned up on Close
do
	local CleanupStore = Nexus.CreateStore({
		Name = "NexusTest_TxvCleanup",
		Schema = TestSchema,
		LockMode = "None",
	})

	local LoadResult = CleanupStore:Load("CleanupKey")
	Assert(LoadResult.Passed == true,
		"Transaction CAS Cleanup: Load succeeded")

	if LoadResult.Passed then
		Assert((CleanupStore :: any)._LastKnownTxv["CleanupKey"] ~= nil,
			"Transaction CAS Cleanup: _LastKnownTxv exists after Load")

		CleanupStore:Close("CleanupKey")

		Assert((CleanupStore :: any)._LastKnownTxv["CleanupKey"] == nil,
			"Transaction CAS Cleanup: _LastKnownTxv cleaned up after Close")
	end

	CleanupStore:Destroy()
end

-- 3c. Single-key transaction succeeds with CAS
do
	local SingleTxStore = Nexus.CreateStore({
		Name = "NexusTest_SingleTx",
		Schema = TestSchema,
		LockMode = "None",
	})

	local LoadResult = SingleTxStore:Load("SingleTxKey")
	Assert(LoadResult.Passed == true,
		"Single Transaction: Load succeeded")

	if LoadResult.Passed then
		local TransactResult = SingleTxStore:Transact({
			{
				Key = "SingleTxKey",
				Transform = function(Data)
					Data.Coins += 100
					return Data
				end,
			},
		})

		Assert(TransactResult.Passed == true,
			"Single Transaction: Transact with CAS succeeded")

		if TransactResult.Passed then
			Assert(TransactResult.Results["SingleTxKey"] ~= nil,
				"Single Transaction: Result entry exists for SingleTxKey")
		end

		-- Verify in-memory session was updated
		local ReadResult = SingleTxStore:Read("SingleTxKey")
		if ReadResult.Passed then
			local Data = (ReadResult :: any).Value
			Assert(Data.Coins == 100,
				"Single Transaction: In-memory data synced after CAS commit",
				`Expected 100, got {Data.Coins}`)
		end

		SingleTxStore:Close("SingleTxKey")
	end

	SingleTxStore:Destroy()
end

-- 3d. Multi-key transaction succeeds with CAS
do
	local MultiTxStore = Nexus.CreateStore({
		Name = "NexusTest_MultiTx",
		Schema = TestSchema,
		LockMode = "None",
	})

	local LoadA = MultiTxStore:Load("MultiA")
	local LoadB = MultiTxStore:Load("MultiB")

	Assert(LoadA.Passed == true and LoadB.Passed == true,
		"Multi Transaction: Both keys loaded")

	if LoadA.Passed and LoadB.Passed then
		local TransactResult = MultiTxStore:Transact({
			{
				Key = "MultiA",
				Transform = function(Data)
					Data.Coins -= 50
					return Data
				end,
			},
			{
				Key = "MultiB",
				Transform = function(Data)
					Data.Coins += 50
					return Data
				end,
			},
		})

		Assert(TransactResult.Passed == true,
			"Multi Transaction: CAS transaction across 2 keys succeeded")

		if TransactResult.Passed then
			local ReadA = MultiTxStore:Read("MultiA")
			local ReadB = MultiTxStore:Read("MultiB")
			if ReadA.Passed and ReadB.Passed then
				local DataA = (ReadA :: any).Value
				local DataB = (ReadB :: any).Value
				Assert(DataA.Coins == -50 and DataB.Coins == 50,
					"Multi Transaction: Both sessions synced correctly",
					`A.Coins={DataA.Coins}, B.Coins={DataB.Coins}`)
			end
		end

		MultiTxStore:Close("MultiA")
		MultiTxStore:Close("MultiB")
	end

	MultiTxStore:Destroy()
end

-- 3e. Transaction aborts when transform returns nil
do
	local AbortStore = Nexus.CreateStore({
		Name = "NexusTest_TxAbort",
		Schema = TestSchema,
		LockMode = "None",
	})

	local LoadResult = AbortStore:Load("AbortKey")
	Assert(LoadResult.Passed == true,
		"Transaction Abort: Load succeeded")

	if LoadResult.Passed then
		local TransactResult = AbortStore:Transact({
			{
				Key = "AbortKey",
				Transform = function(_Data)
					return nil -- deliberately abort
				end,
			},
		})

		Assert(TransactResult.Passed == false,
			"Transaction Abort: Transaction fails when transform returns nil")

		if TransactResult.Error then
			Assert(TransactResult.Error.Kind == "TransactionAborted",
				"Transaction Abort: Error kind is TransactionAborted",
				`Got {TransactResult.Error.Kind}`)
		end

		AbortStore:Close("AbortKey")
	end

	AbortStore:Destroy()
end

-- 3f. _CommitParticipantCAS is accessible as a method
do
	local MethodStore = Nexus.CreateStore({
		Name = "NexusTest_CommitMethod",
		Schema = TestSchema,
		LockMode = "None",
	})

	Assert(typeof((MethodStore :: any)._CommitParticipantCAS) == "function",
		"_CommitParticipantCAS exists as a method on the Store object")

	MethodStore:Destroy()
end

-- ══════════════════════════════════════════════════════════════════════════════
-- SUMMARY
-- ══════════════════════════════════════════════════════════════════════════════

Section("TEST RESULTS")
print(`Total: {TestCount}  |  Passed: {PassCount}  |  Failed: {FailCount}`)
if FailCount == 0 then
	print(`\n{PASS} ALL TESTS PASSED`)
else
	warn(`\n{FAIL} {FailCount} TEST(S) FAILED`)
end
