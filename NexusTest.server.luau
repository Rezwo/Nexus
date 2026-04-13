--!strict
local Nexus = require(game.ReplicatedStorage.Nexus)

local PASS = "\226\156\133"
local FAIL = "\226\157\140"
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

local TestSchema = {
	Version = 1,
	Default = { Coins = 0, Gems = 0, Inventory = {} },
}

local ValidatedSchema = {
	Version = 1,
	Default = { Coins = 0, Gems = 0, Inventory = {} },
	Validate = function(Data : any) : (boolean, string?)
		if typeof(Data.Coins) ~= "number" then return false, "Coins must be a number" end
		if Data.Coins < 0 then return false, "Coins cannot be negative" end
		return true, nil
	end,
}

local MigratableSchema = {
	Version = 2,
	Default = { Coins = 0, Gems = 0, Level = 1, Inventory = {} },
	Migrations = {
		{ Version = 2, Up = function(Data : any)
			Data.Level = Data.Level or 1
			return Data
		end },
	},
}

Section("1. Store Lifecycle")

do
	local Store = Nexus.CreateStore({ Name = "NexusTest_Lifecycle", Schema = TestSchema, LockMode = "None" })

	local LoadResult = Store:Load("LifecycleKey")
	Assert(LoadResult.Passed == true, "Load returns Passed")

	if LoadResult.Passed then
		local Data = (LoadResult :: any).Value
		Assert(Data.Coins == 0, "Default data applied on first load")

		Assert(Store:IsLoaded("LifecycleKey") == true, "IsLoaded returns true for loaded key")
		Assert(Store:IsLoaded("NonexistentKey") == false, "IsLoaded returns false for unloaded key")

		local Keys = Store:GetActiveKeys()
		Assert(#Keys == 1 and Keys[1] == "LifecycleKey", "GetActiveKeys returns loaded keys")

		local Info = Store:GetSessionInfo("LifecycleKey")
		Assert(Info ~= nil, "GetSessionInfo returns info for loaded key")
		if Info then
			Assert(Info.Status == "Active", "Session status is Active after load")
			Assert(Info.Dirty == false, "Session is not dirty after load")
		end

		local NilInfo = Store:GetSessionInfo("NonexistentKey")
		Assert(NilInfo == nil, "GetSessionInfo returns nil for unloaded key")

		Store:Close("LifecycleKey")
		Assert(Store:IsLoaded("LifecycleKey") == false, "IsLoaded returns false after Close")
	end

	Store:Destroy()
end

Section("2. CRUD Operations")

do
	local Store = Nexus.CreateStore({ Name = "NexusTest_CRUD", Schema = TestSchema, LockMode = "None" })

	local LoadResult = Store:Load("CRUDKey")
	Assert(LoadResult.Passed == true, "CRUD: Load succeeded")

	if LoadResult.Passed then
		local ReadResult = Store:Read("CRUDKey")
		Assert(ReadResult.Passed == true, "Read returns data")
		if ReadResult.Passed then
			Assert((ReadResult :: any).Value.Coins == 0, "Read returns default data")
		end

		local UpdateResult = Store:Update("CRUDKey", function(Data)
			Data.Coins = 100
			Data.Gems = 50
			return Data
		end)
		Assert(UpdateResult.Passed == true, "Update succeeded")

		local ReadAfterUpdate = Store:Read("CRUDKey")
		if ReadAfterUpdate.Passed then
			local Data = (ReadAfterUpdate :: any).Value
			Assert(Data.Coins == 100, "Read returns updated Coins")
			Assert(Data.Gems == 50, "Read returns updated Gems")
		end

		local SaveResult = Store:Save("CRUDKey")
		Assert(SaveResult.Passed == true, "Save succeeded")

		Store:Close("CRUDKey")
	end

	local PeekResult = Store:Peek("CRUDKey")
	Assert(PeekResult.Passed == true, "Peek succeeds on closed key")
	if PeekResult.Passed then
		Assert((PeekResult :: any).Value.Coins == 100, "Peek returns persisted data")
	end

	Store:Destroy()
end

Section("3. Double Load Guard")

do
	local Store = Nexus.CreateStore({ Name = "NexusTest_DoubleLoad", Schema = TestSchema, LockMode = "None" })

	local Load1 = Store:Load("DoubleKey")
	Assert(Load1.Passed == true, "First load succeeds")

	local Load2 = Store:Load("DoubleKey")
	Assert(Load2.Passed == false, "Double load is rejected")
	if not Load2.Passed then
		Assert(Nexus.IsSessionLocked(Load2) == true, "Double load returns SessionLocked error")
	end

	Store:Close("DoubleKey")
	local Load3 = Store:Load("DoubleKey")
	Assert(Load3.Passed == true, "Load succeeds after Close")

	Store:Close("DoubleKey")
	Store:Destroy()
end

Section("4. Close Without Load")

do
	local Store = Nexus.CreateStore({ Name = "NexusTest_CloseNoLoad", Schema = TestSchema, LockMode = "None" })

	local CloseResult = Store:Close("NeverLoadedKey")
	Assert(CloseResult.Passed == false, "Close on unloaded key fails")
	if not CloseResult.Passed then
		Assert(Nexus.IsSessionNotFound(CloseResult) == true, "Returns SessionNotFound")
	end

	Store:Destroy()
end

Section("5. Read/Update Without Load")

do
	local Store = Nexus.CreateStore({ Name = "NexusTest_NoSession", Schema = TestSchema, LockMode = "None" })

	local ReadResult = Store:Read("GhostKey")
	Assert(ReadResult.Passed == false, "Read on unloaded key fails")

	local UpdateResult = Store:Update("GhostKey", function(Data) Data.Coins = 1; return Data end)
	Assert(UpdateResult.Passed == false, "Update on unloaded key fails")

	local SaveResult = Store:Save("GhostKey")
	Assert(SaveResult.Passed == false, "Save on unloaded key fails")

	Store:Destroy()
end

Section("6. Schema Validation")

do
	local Store = Nexus.CreateStore({ Name = "NexusTest_Validation", Schema = ValidatedSchema, LockMode = "None" })

	local LoadResult = Store:Load("ValidKey")
	Assert(LoadResult.Passed == true, "Validated load succeeds")

	if LoadResult.Passed then
		local GoodUpdate = Store:Update("ValidKey", function(Data)
			Data.Coins = 50
			return Data
		end)
		Assert(GoodUpdate.Passed == true, "Valid update passes validation")

		local BadUpdate = Store:Update("ValidKey", function(Data)
			Data.Coins = -10
			return Data
		end)
		Assert(BadUpdate.Passed == false, "Negative Coins fails validation")
		if not BadUpdate.Passed then
			Assert(Nexus.IsValidationError(BadUpdate) == true, "Error kind is ValidationFailed")
		end

		local AfterBad = Store:Read("ValidKey")
		if AfterBad.Passed then
			Assert((AfterBad :: any).Value.Coins == 50, "Data unchanged after failed validation")
		end

		Store:Close("ValidKey")
	end

	Store:Destroy()
end

Section("7. Schema Migration")

do
	local StoreV1 = Nexus.CreateStore({ Name = "NexusTest_Migration", Schema = TestSchema, LockMode = "None" })
	local Load1 = StoreV1:Load("MigrateKey")
	if Load1.Passed then
		StoreV1:Update("MigrateKey", function(Data) Data.Coins = 500; return Data end)
		StoreV1:Save("MigrateKey")
		StoreV1:Close("MigrateKey")
	end
	StoreV1:Destroy()

	local StoreV2 = Nexus.CreateStore({ Name = "NexusTest_Migration", Schema = MigratableSchema, LockMode = "None" })
	local Load2 = StoreV2:Load("MigrateKey")
	Assert(Load2.Passed == true, "V2 load with migration succeeds")
	if Load2.Passed then
		local Data = (Load2 :: any).Value
		Assert(Data.Coins == 500, "Coins preserved across migration")
		Assert(Data.Level == 1, "Migration added default Level field")
		StoreV2:Close("MigrateKey")
	end
	StoreV2:Destroy()
end

Section("8. QueueAction + SaveOptimistic")

do
	local Store = Nexus.CreateStore({ Name = "NexusTest_Optimistic", Schema = TestSchema, LockMode = "None" })

	local LoadResult = Store:Load("OptiKey")
	Assert(LoadResult.Passed == true, "Optimistic: Load succeeded")

	if LoadResult.Passed then
		local QueueResult = Store:QueueAction("OptiKey", function(Data)
			Data.Coins += 10
			return Data
		end)
		Assert(QueueResult.Passed == true, "QueueAction succeeds")

		Store:QueueAction("OptiKey", function(Data)
			Data.Coins += 20
			return Data
		end)

		local OptResult = Store:SaveOptimistic("OptiKey")
		Assert(OptResult.Passed == true, "SaveOptimistic succeeds")

		local ReadResult = Store:Read("OptiKey")
		if ReadResult.Passed then
			Assert((ReadResult :: any).Value.Coins == 30, "Optimistic save applied queued actions (10+20)")
		end

		Store:Close("OptiKey")
	end

	Store:Destroy()
end

Section("9. SaveOptimistic Sharded Fallback")

do
	local Store = Nexus.CreateStore({ Name = "NexusTest_OptShard", Schema = TestSchema, LockMode = "None" })

	local LoadResult = Store:Load("ShardOptiKey")
	Assert(LoadResult.Passed == true, "Shard fallback: Load succeeded")

	if LoadResult.Passed then
		(Store :: any)._ShardCounts["ShardOptiKey"] = 2

		Store:QueueAction("ShardOptiKey", function(Data)
			Data.Coins = 999
			return Data
		end)

		local OptResult = Store:SaveOptimistic("ShardOptiKey")
		Assert(OptResult.Passed == true, "SaveOptimistic on sharded key falls back gracefully")

		Store:Close("ShardOptiKey")
	end

	Store:Destroy()
end

Section("10. Signals and Callbacks")

do
	local Store = Nexus.CreateStore({ Name = "NexusTest_Signals", Schema = TestSchema, LockMode = "None" })

	local LoadFired = false
	local SaveFired = false
	local ReleaseFired = false

	Store.OnLoad:Connect(function() LoadFired = true end)
	Store.OnSave:Connect(function() SaveFired = true end)
	Store.OnRelease:Connect(function() ReleaseFired = true end)

	local CallbackLoadFired = false
	local CallbackSaveFired = false
	Store:SetCallbacks({
		OnLoad = function() CallbackLoadFired = true end,
		OnSave = function() CallbackSaveFired = true end,
	})

	local LoadResult = Store:Load("SignalKey")
	if LoadResult.Passed then
		Assert(LoadFired == true, "OnLoad signal fired")
		Assert(CallbackLoadFired == true, "OnLoad callback fired")

		Store:Update("SignalKey", function(Data) Data.Coins = 1; return Data end)
		Store:Save("SignalKey")
		Assert(SaveFired == true, "OnSave signal fired")
		Assert(CallbackSaveFired == true, "OnSave callback fired")

		Store:Close("SignalKey")
		Assert(ReleaseFired == true, "OnRelease signal fired")
	end

	Store:Destroy()
end

Section("11. Middleware")

do
	local Store = Nexus.CreateStore({ Name = "NexusTest_Middleware", Schema = TestSchema, LockMode = "None" })

	local LoadMiddlewareCalled = false
	local SaveMiddlewareCalled = false

	Store:Use("load", function(_Key, Data)
		LoadMiddlewareCalled = true
		return Data
	end)

	Store:Use("save", function(_Key, _Data)
		SaveMiddlewareCalled = true
		return nil
	end)

	local LoadResult = Store:Load("MWKey")
	if LoadResult.Passed then
		Assert(LoadMiddlewareCalled == true, "Load middleware executed")

		Store:Update("MWKey", function(Data) Data.Coins = 1; return Data end)
		Store:Save("MWKey")
		Assert(SaveMiddlewareCalled == true, "Save middleware executed")

		Store:Close("MWKey")
	end

	Store:Destroy()
end

Section("12. Soft Delete and Recover")

do
	local Store = Nexus.CreateStore({ Name = "NexusTest_SoftDelete", Schema = TestSchema, LockMode = "None" })

	local LoadResult = Store:Load("SoftDelKey")
	if LoadResult.Passed then
		Store:Update("SoftDelKey", function(Data) Data.Coins = 777; return Data end)
		Store:Save("SoftDelKey")
		Store:Close("SoftDelKey")
	end

	local DeleteResult = Store:SoftDelete("SoftDelKey")
	Assert(DeleteResult.Passed == true, "SoftDelete succeeds")

	Assert(Store:IsSoftDeleted("SoftDelKey") == true, "IsSoftDeleted returns true")

	local LoadDeleted = Store:Load("SoftDelKey")
	Assert(LoadDeleted.Passed == false, "Load on soft-deleted key fails")

	local RecoverResult = Store:Recover("SoftDelKey")
	Assert(RecoverResult.Passed == true, "Recover succeeds")
	if RecoverResult.Passed then
		Assert((RecoverResult :: any).Value.Coins == 777, "Recovered data is intact")
	end

	Assert(Store:IsSoftDeleted("SoftDelKey") == false, "IsSoftDeleted returns false after recovery")

	Store:Destroy()
end

Section("13. Version History and Rollback")

do
	local Store = Nexus.CreateStore({ Name = "NexusTest_Versions", Schema = TestSchema, LockMode = "None" })

	local LoadResult = Store:Load("VersionKey")
	if LoadResult.Passed then
		Store:Update("VersionKey", function(Data) Data.Coins = 100; return Data end)
		Store:Save("VersionKey")
		Store:Update("VersionKey", function(Data) Data.Coins = 200; return Data end)
		Store:Save("VersionKey")
		Store:Close("VersionKey")
	end

	local VersionsResult = Store:ListVersions("VersionKey")
	Assert(VersionsResult.Passed == true, "ListVersions succeeds")
	if VersionsResult.Passed then
		local Versions = (VersionsResult :: any).Value
		Assert(#Versions >= 1, "At least 1 version exists", tostring(#Versions))

		if #Versions >= 1 then
			local LatestVersion = Versions[1]
			local GetVersionResult = Store:GetVersion("VersionKey", LatestVersion.Version)
			Assert(GetVersionResult.Passed == true, "GetVersion succeeds")
		end
	end

	Store:Destroy()
end

Section("14. ListKeys")

do
	local Store = Nexus.CreateStore({ Name = "NexusTest_ListKeys", Schema = TestSchema, LockMode = "None" })

	for Index = 1, 3 do
		local Key = "ListKey_" .. tostring(Index)
		local Load = Store:Load(Key)
		if Load.Passed then
			Store:Update(Key, function(Data) Data.Coins = Index; return Data end)
			Store:Save(Key)
			Store:Close(Key)
		end
	end

	local ListResult = Store:ListKeys("ListKey_")
	Assert(ListResult.Passed == true, "ListKeys succeeds")
	if ListResult.Passed then
		local Keys = (ListResult :: any).Value
		Assert(#Keys >= 3, "ListKeys returns at least 3 keys", tostring(#Keys))
	end

	Store:Destroy()
end

Section("15. MemoryBudget Isolation")

do
	Nexus.MemoryBudget.Reset()

	for _ = 1, 6 do
		Nexus.MemoryBudget.RecordFailure("StoreA")
	end

	local StatusA = Nexus.MemoryBudget.GetStatus("StoreA")
	local StatusB = Nexus.MemoryBudget.GetStatus("StoreB")

	Assert(StatusA.InBackoff == true, "StoreA is in backoff after 6 failures")
	Assert(StatusB.InBackoff == false, "StoreB is NOT in backoff (isolated)")
	Assert(StatusA.RecentFailureCount >= 5, "StoreA has >= 5 recent failures", tostring(StatusA.RecentFailureCount))
	Assert(StatusB.RecentFailureCount == 0, "StoreB has 0 recent failures")

	Nexus.MemoryBudget.RecordSuccess("StoreA")
	local StatusAfterSuccess = Nexus.MemoryBudget.GetStatus("StoreA")
	Assert(StatusAfterSuccess.RecentFailureCount < StatusA.RecentFailureCount,
		"RecordSuccess erodes failure count",
		`Before={StatusA.RecentFailureCount}, After={StatusAfterSuccess.RecentFailureCount}`)

	Nexus.MemoryBudget.Reset("StoreA")
	local StatusAfterReset = Nexus.MemoryBudget.GetStatus("StoreA")
	Assert(StatusAfterReset.RecentFailureCount == 0, "Reset clears failures for StoreA")
	Assert(StatusAfterReset.InBackoff == false, "Reset clears backoff for StoreA")

	Nexus.MemoryBudget.Reset()
end

Section("16. Transaction CAS (_txv)")

do
	local Store = Nexus.CreateStore({ Name = "NexusTest_Transaction", Schema = TestSchema, LockMode = "None" })

	local LoadA = Store:Load("TxnKeyA")
	local LoadB = Store:Load("TxnKeyB")
	Assert(LoadA.Passed == true, "Txn: Load A succeeded")
	Assert(LoadB.Passed == true, "Txn: Load B succeeded")

	if LoadA.Passed and LoadB.Passed then
		local TxvA = (Store :: any)._LastKnownTxv["TxnKeyA"]
		Assert(TxvA ~= nil, "TxnKeyA has _txv after load")

		local TxnResult = Store:Transact({
			{ Key = "TxnKeyA", Transform = function(Data) Data.Coins += 50; return Data end },
			{ Key = "TxnKeyB", Transform = function(Data) Data.Gems += 25; return Data end },
		})
		Assert(TxnResult.Passed == true, "Multi-key transaction succeeds")

		if TxnResult.Passed then
			local ReadA = Store:Read("TxnKeyA")
			local ReadB = Store:Read("TxnKeyB")
			if ReadA.Passed then Assert((ReadA :: any).Value.Coins == 50, "TxnKeyA Coins updated") end
			if ReadB.Passed then Assert((ReadB :: any).Value.Gems == 25, "TxnKeyB Gems updated") end

			local NewTxvA = (Store :: any)._LastKnownTxv["TxnKeyA"]
			Assert(NewTxvA > TxvA, "TxnKeyA _txv incremented after transaction")
		end

		local AbortResult = Store:Transact({
			{ Key = "TxnKeyA", Transform = function(_Data) return nil end },
		})
		Assert(AbortResult.Passed == false, "Transaction with nil transform aborts")

		Store:Close("TxnKeyA")
		Store:Close("TxnKeyB")
	end

	Store:Destroy()
end

Section("17. Audit Trail - Ephemeral")

do
	local Store = Nexus.CreateStore({
		Name = "NexusTest_AuditEphemeral",
		Schema = TestSchema,
		LockMode = "None",
		AuditLog = true,
		AuditLogSize = 100,
	})

	local LoadResult = Store:Load("AuditKey")
	if LoadResult.Passed then
		Store:Update("AuditKey", function(Data) Data.Coins = 42; return Data end)
		Store:Save("AuditKey")

		local AuditResult = Store:GetAuditLog("AuditKey")
		Assert(AuditResult.Passed == true, "GetAuditLog succeeds")
		if AuditResult.Passed then
			local Entries = (AuditResult :: any).Value
			Assert(#Entries >= 3, "Audit has Load+Update+Save entries", tostring(#Entries))

			local HasLoad = false
			local HasUpdate = false
			local HasSave = false
			for _, Entry in Entries do
				if Entry.Operation == "Load" then HasLoad = true end
				if Entry.Operation == "Update" then HasUpdate = true end
				if Entry.Operation == "Save" then HasSave = true end
			end
			Assert(HasLoad, "Audit contains Load entry")
			Assert(HasUpdate, "Audit contains Update entry")
			Assert(HasSave, "Audit contains Save entry")
		end

		Store:Close("AuditKey")

		local LeakCheck = (Store :: any)._AuditLogs["AuditKey"]
		Assert(LeakCheck == nil, "Audit memory freed after Close (leak fix)")
	end

	Store:Destroy()
end

Section("18. Audit Trail - Persistent Round-Trip")

do
	local Store1 = Nexus.CreateStore({
		Name = "NexusTest_AuditPersist",
		Schema = TestSchema,
		LockMode = "None",
		AuditLog = true,
		AuditLogSize = 500,
		AuditPersistence = true,
	})

	local Load1 = Store1:Load("PersistAuditKey")
	if Load1.Passed then
		Store1:Update("PersistAuditKey", function(Data) Data.Coins = 100; return Data end)
		Store1:Save("PersistAuditKey")
		Store1:Close("PersistAuditKey")
	end
	Store1:Destroy()

	local Store2 = Nexus.CreateStore({
		Name = "NexusTest_AuditPersist",
		Schema = TestSchema,
		LockMode = "None",
		AuditLog = true,
		AuditLogSize = 500,
		AuditPersistence = true,
	})

	local RemoteAudit = Store2:GetAuditLogRemote("PersistAuditKey")
	Assert(RemoteAudit.Passed == true, "GetAuditLogRemote succeeds")
	if RemoteAudit.Passed then
		local Entries = (RemoteAudit :: any).Value
		Assert(#Entries > 0, "Remote audit has entries from previous session", tostring(#Entries))
	end
	Store2:Destroy()
end

Section("19. Audit Trimming")

do
	local Store = Nexus.CreateStore({
		Name = "NexusTest_AuditTrim",
		Schema = TestSchema,
		LockMode = "None",
		AuditLog = true,
		AuditLogSize = 5,
		AuditPersistence = true,
	})

	local LoadResult = Store:Load("TrimKey")
	if LoadResult.Passed then
		for I = 1, 10 do
			Store:Update("TrimKey", function(Data) Data.Coins = I; return Data end)
		end

		local AuditResult = Store:GetAuditLog("TrimKey")
		if AuditResult.Passed then
			Assert(#(AuditResult :: any).Value <= 5, "In-memory audit trimmed to AuditLogSize", tostring(#(AuditResult :: any).Value))
		end

		Store:Save("TrimKey")
		Store:Close("TrimKey")
	end

	local CheckStore = Nexus.CreateStore({
		Name = "NexusTest_AuditTrim",
		Schema = TestSchema,
		LockMode = "None",
		AuditLog = true,
		AuditLogSize = 5,
		AuditPersistence = true,
	})
	local RemoteAudit = CheckStore:GetAuditLogRemote("TrimKey")
	if RemoteAudit.Passed then
		Assert(#(RemoteAudit :: any).Value <= 5, "Remote audit also trimmed", tostring(#(RemoteAudit :: any).Value))
	end
	CheckStore:Destroy()
	Store:Destroy()
end

Section("20. Remove Cleans Up Everything")

do
	local Store = Nexus.CreateStore({
		Name = "NexusTest_Remove",
		Schema = TestSchema,
		LockMode = "None",
		AuditLog = true,
		AuditLogSize = 100,
		AuditPersistence = true,
	})

	local LoadResult = Store:Load("RemoveKey")
	if LoadResult.Passed then
		Store:Update("RemoveKey", function(Data) Data.Coins = 999; return Data end)
		Store:Save("RemoveKey")
		Store:Close("RemoveKey")
	end

	local RemoveResult = Store:Remove("RemoveKey")
	Assert(RemoveResult.Passed == true, "Remove succeeds")

	local PeekResult = Store:Peek("RemoveKey")
	if PeekResult.Passed then
		Assert((PeekResult :: any).Value == nil or (PeekResult :: any).Value.Coins == 0,
			"Data is gone or default after Remove")
	end

	local AuditAfter = Store:GetAuditLogRemote("RemoveKey")
	if AuditAfter.Passed then
		Assert(#(AuditAfter :: any).Value == 0, "Remote audit cleaned up after Remove")
	end

	Store:Destroy()
end

Section("21. CloseAll")

do
	local Store = Nexus.CreateStore({ Name = "NexusTest_CloseAll", Schema = TestSchema, LockMode = "None" })

	Store:Load("CA_1")
	Store:Load("CA_2")
	Store:Load("CA_3")

	Assert(#Store:GetActiveKeys() == 3, "3 keys loaded before CloseAll")

	Store:CloseAll()

	Assert(#Store:GetActiveKeys() == 0, "0 keys after CloseAll")
	Store:Destroy()
end

Section("22. LoadMany")

do
	local Store = Nexus.CreateStore({ Name = "NexusTest_LoadMany", Schema = TestSchema, LockMode = "None" })

	for I = 1, 3 do
		local Key = "LM_" .. tostring(I)
		local Load = Store:Load(Key)
		if Load.Passed then
			Store:Update(Key, function(Data) Data.Coins = I * 10; return Data end)
			Store:Save(Key)
			Store:Close(Key)
		end
	end

	local BatchResult = Store:LoadMany({ "LM_1", "LM_2", "LM_3" })
	Assert(BatchResult.Succeeded["LM_1"] ~= nil, "LoadMany: LM_1 loaded")
	Assert(BatchResult.Succeeded["LM_2"] ~= nil, "LoadMany: LM_2 loaded")
	Assert(BatchResult.Succeeded["LM_3"] ~= nil, "LoadMany: LM_3 loaded")

	if BatchResult.Succeeded["LM_1"] then
		Assert(BatchResult.Succeeded["LM_1"].Coins == 10, "LoadMany: LM_1 has correct data")
	end

	Store:CloseAll()
	Store:Destroy()
end

Section("23. Telemetry")

do
	local Store = Nexus.CreateStore({ Name = "NexusTest_Telemetry", Schema = TestSchema, LockMode = "None" })

	local Telemetry = Store:GetTelemetry()
	Assert(Telemetry ~= nil, "GetTelemetry returns a scope")

	local Load = Store:Load("TelemetryKey")
	if Load.Passed then
		Store:Save("TelemetryKey")
		Store:Close("TelemetryKey")
	end

	local Report = Telemetry:GetReport()
	local LoadOp = Report["Store.Load"]
	Assert(LoadOp ~= nil, "Telemetry recorded Store.Load stats")
	if LoadOp then
		Assert(LoadOp.TotalCount >= 1, "At least 1 Load call recorded")
	end

	Store:Destroy()
end

Section("24. Transform Returns Nil (No-Op)")

do
	local Store = Nexus.CreateStore({ Name = "NexusTest_NilTransform", Schema = TestSchema, LockMode = "None" })

	local Load = Store:Load("NilTxKey")
	if Load.Passed then
		Store:Update("NilTxKey", function(Data) Data.Coins = 42; return Data end)

		local NilUpdate = Store:Update("NilTxKey", function(_Data)
			return nil
		end)

		local ReadResult = Store:Read("NilTxKey")
		if ReadResult.Passed then
			Assert((ReadResult :: any).Value.Coins == 42, "Data unchanged after nil-returning transform")
		end

		Store:Close("NilTxKey")
	end

	Store:Destroy()
end

Section("25. Error Helpers")

do
	local FakeThrottled = { Passed = false, Error = { Kind = "Throttled", Message = "test" } }
	local FakeDS = { Passed = false, Error = { Kind = "DataStoreError", Message = "test" } }
	local FakeValidation = { Passed = false, Error = { Kind = "ValidationFailed", Message = "test" } }
	local FakeBudget = { Passed = false, Error = { Kind = "BudgetExhausted", Message = "test" } }
	local FakeSuccess = { Passed = true, Value = true }

	Assert(Nexus.IsThrottled(FakeThrottled :: any) == true, "IsThrottled detects Throttled")
	Assert(Nexus.IsDataStoreError(FakeDS :: any) == true, "IsDataStoreError detects DataStoreError")
	Assert(Nexus.IsValidationError(FakeValidation :: any) == true, "IsValidationError detects ValidationFailed")
	Assert(Nexus.IsBudgetExhausted(FakeBudget :: any) == true, "IsBudgetExhausted detects BudgetExhausted")
	Assert(Nexus.IsThrottled(FakeSuccess :: any) == false, "IsThrottled returns false on success")
	Assert(Nexus.IsSessionLocked(FakeThrottled :: any) == false, "IsSessionLocked returns false on wrong kind")
end

Section("RESULTS")
print(`Total: {TestCount}  |  Passed: {PassCount}  |  Failed: {FailCount}`)
if FailCount == 0 then
	print(`\n{PASS} ALL TESTS PASSED`)
else
	warn(`\n{FAIL} {FailCount} TEST(S) FAILED`)
end
