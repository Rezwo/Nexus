--!strict
local ReplicatedStorage = game:GetService("ReplicatedStorage")
local NexusTestHelpers = require(ReplicatedStorage.Nexus.NexusTestHelpers)
local Nexus = NexusTestHelpers.Nexus
local MockDataStore = NexusTestHelpers.MockDataStore
local Assert = NexusTestHelpers.Assert
local Section = NexusTestHelpers.Section
local CombinedSummary = NexusTestHelpers.CombinedSummary

local TestSchema = {
	Version = 1,
	Default = { Coins = 0, Gems = 0, Inventory = {} },
}

Section("F-ERROR-SHAPES: DataStore Errors")
do
	local Harness = NexusTestHelpers.TestHarness.New()
	local Store = Harness:CreateStore({ Name = "Failures_ErrorShapes", Schema = TestSchema })
	
	MockDataStore.Fault("Failures_ErrorShapes:global", "GetAsync", "Fake DataStore throttle")
	local LoadResult = Store:Load("ErrShapeKey")
	Assert(LoadResult.Passed == false, "[expected] Load fails due to generic DataStoreError")
	Assert(Nexus.IsDataStoreError(LoadResult), "Result is DataStoreError")
	
	Harness:Teardown()
end

Section("F-TRANSACTION: Conflicts and Aborts")
do
	local Harness = NexusTestHelpers.TestHarness.New()
	local StoreA = Harness:CreateStore({ Name = "TxnConflicts_A", Schema = TestSchema })
	
	local LoadA = StoreA:Load("KeyA")
	Assert(LoadA.Passed, "Txn KeyA Loaded")
	
	local TxnResult = Nexus.Transact({
		{ Store = StoreA, Key = "KeyA", Transform = function(d) return nil end }
	})
	Assert(TxnResult.Passed == false, "[expected] Transaction aborts")
	
	Harness:Teardown()
end

Section("F-BUDGET: Exhaustion and Recovery")
do
	local Harness = NexusTestHelpers.TestHarness.New()
	local Store = Harness:CreateStore({ Name = "Failures_Budget", Schema = TestSchema })
	
	for i = 1, 10 do
		Nexus.MemoryBudget.RecordFailure("Failures_Budget")
	end
	
	local LoadResult = Store:Load("BudgetKey")
	Assert(LoadResult.Passed == false, "[expected] Load fails due to budget constraint")
	Assert(Nexus.IsBudgetExhausted(LoadResult), "Result is BudgetExhausted")
	
	for i = 1, 3 do
		Nexus.MemoryBudget.RecordSuccess("Failures_Budget")
	end
	
	local LoadResult2 = Store:Load("BudgetKey")
	Assert(LoadResult2.Passed, "Load succeeds after budget recovery")
	
	Harness:Teardown()
end

Section("F-CIRCUIT: Breaker Transitions")
do
	local Harness = NexusTestHelpers.TestHarness.New()
	local Store = Harness:CreateStore({ Name = "Failures_Circuit", Schema = TestSchema })
	
	for i=1, 6 do
		MockDataStore.Fault("Failures_Circuit:global", "GetAsync", "Trigger Circuit")
		local r = Store:Load("CircKey" .. i)
		Assert(r.Passed == false, "[expected] Circuit load " .. i .. " fails")
	end
	
	Harness:Teardown()
end

Section("F-SHARD: Partial Reads/Writes")
do
	local Harness = NexusTestHelpers.TestHarness.New()
	local Store = Harness:CreateStore({ Name = "Failures_Shard", Schema = TestSchema })
	
	local ShardDS = MockDataStore.GetOrCreate("Failures_Shard", "global")
	ShardDS:Seed("ShardKey", {
		__shards = 5,
		__v = 1
	})
	ShardDS:Seed("ShardKey_shard_1", "part1")
	
	local LoadR = Store:Load("ShardKey")
	Assert(LoadR.Passed == false, "[expected] Load fails when a shard is missing")
	
	Harness:Teardown()
end

Section("F-RETRY: Max Retries")
do
	local Harness = NexusTestHelpers.TestHarness.New()
	local Store = Harness:CreateStore({ Name = "Failures_Retry", Schema = TestSchema })
	
	for i=1, 10 do
		MockDataStore.Fault("Failures_Retry:global", "GetAsync", "Continuous Failure " .. i)
	end
	
	local LoadResult = Store:Load("RetryKey")
	Assert(LoadResult.Passed == false, "[expected] Load gives up after max retries")
	
	Harness:Teardown()
end

Section("F-INBOX: Update failures")
do
	local Harness = NexusTestHelpers.TestHarness.New()
	local Inbox = Harness:CreateStore({ Name = "Failures_Inbox", Schema = TestSchema })
	
	MockDataStore.Fault("Failures_Inbox:global", "UpdateAsync", "Inbox Update Fail")
	local Res = Inbox:Update("InboxKey", function(d) return d end)
	Assert(Res.Passed == false, "[expected] Inbox updates can fail")
	
	Harness:Teardown()
end
