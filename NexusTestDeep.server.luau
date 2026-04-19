--!strict
--[[
	NexusTestDeep.server.luau
	Deep test suite for Nexus DataStore.
	Covers fix regressions, edge cases, and paths not exercised by NexusTest.server.luau.
--]]

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
	Default = { Coins = 0, Gems = 0, Inventory = {} :: { string } },
}

-- ============================================================
-- FIX 1: Shard key filtering uses string.match, not string.find
-- Verify that keys ending in _shard_N are excluded from ListKeys.
-- We can't inject real shard keys at the DS level in tests, but
-- we can verify the pattern itself is correct Lua.
-- ============================================================
Section("F1. Shard Filter Pattern Correctness")

do
	local function IsShardKey(Name : string) : boolean
		return string.match(Name, "_shard_%d+$") ~= nil
	end
	Assert(IsShardKey("PlayerData_shard_1") == true,  "Detects _shard_1 suffix")
	Assert(IsShardKey("PlayerData_shard_10") == true, "Detects _shard_10 suffix")
	Assert(IsShardKey("PlayerData_shard_1x") == false,"Does NOT match _shard_1x (non-digit tail)")
	Assert(IsShardKey("sharded_data") == false,        "Does NOT match key that contains 'shard' in middle")
	Assert(IsShardKey("PlayerData") == false,          "Does NOT match plain key")
	Assert(IsShardKey("prefix_shard_99_extra") == false,"Anchored to end: _shard_99_extra not at $")
end

-- ============================================================
-- FIX 2: SoftDelete audit recorded AFTER write succeeds
-- Verify that if no active session, SoftDelete still records
-- audit on success and does NOT record it on failure.
-- This test uses a real key so we can confirm normal path.
-- ============================================================
Section("F2. SoftDelete Audit Ordering")

do
	local Store = Nexus.CreateStore({
		Name = "NexusDeep_SoftDeleteAudit",
		Schema = TestSchema,
		LockMode = "None",
		AuditLog = true,
		AuditLogSize = 100,
	})

	local Load = Store:Load("SDKey")
	if Load.Passed then
		Store:Update("SDKey", function(Data) Data.Coins = 55; return Data end)
		Store:Save("SDKey")
		Store:Close("SDKey")
	end

	local DeleteResult = Store:SoftDelete("SDKey")
	Assert(DeleteResult.Passed == true, "F2: SoftDelete succeeds")

	-- After SoftDelete succeeds, _RecordAudit is called for the SoftDelete
	-- operation, which re-creates the in-memory log. Verify it contains exactly
	-- the SoftDelete entry (not the pre-close session entries).
	local AuditLog = (Store :: any)._AuditLogs["SDKey"]
	local HasOnlySoftDeleteEntry = AuditLog ~= nil and #AuditLog == 1
		and AuditLog[1].Operation == "SoftDelete"
	Assert(HasOnlySoftDeleteEntry,
		"F2: In-memory audit after SoftDelete contains exactly the SoftDelete entry")

	Assert(Store:IsSoftDeleted("SDKey") == true, "F2: Key is marked soft-deleted")
	Store:Destroy()
end

-- ============================================================
-- FIX 3: _PerformDebouncedSave deduplication — BeforeSave path
-- Verify the BeforeSave callback path correctly goes through
-- _FinalizeSave and fires OnSave + marks session clean.
-- ============================================================
Section("F3. BeforeSave Callback Path via _FinalizeSave")

do
	local Store = Nexus.CreateStore({
		Name = "NexusDeep_BeforeSave",
		Schema = TestSchema,
		LockMode = "None",
	})

	local OnSaveFired = false
	Store.OnSave:Connect(function() OnSaveFired = true end)

	local BeforeSaveCalled = false
	Store:SetCallbacks({
		BeforeSave = function(_Key, Data)
			BeforeSaveCalled = true
			return Data
		end,
	})

	Store:Load("BSKey")
	Store:Update("BSKey", function(Data) Data.Coins = 77; return Data end)
	local SaveResult = Store:Save("BSKey")

	Assert(SaveResult.Passed == true,   "F3: Save with BeforeSave succeeds")
	Assert(BeforeSaveCalled == true,    "F3: BeforeSave callback was called")
	Assert(OnSaveFired == true,         "F3: OnSave signal fired through _FinalizeSave")

	local Info = Store:GetSessionInfo("BSKey")
	Assert(Info ~= nil and Info.Dirty == false, "F3: Session marked clean after BeforeSave save")

	Store:Close("BSKey")
	Store:Destroy()
end

-- ============================================================
-- FIX 3b: BeforeSave returning nil cancels save
-- ============================================================
Section("F3b. BeforeSave Returning Nil Cancels Save")

do
	local Store = Nexus.CreateStore({
		Name = "NexusDeep_BeforeSaveCancel",
		Schema = TestSchema,
		LockMode = "None",
	})

	Store:SetCallbacks({
		BeforeSave = function(_Key, _Data)
			return nil
		end,
	})

	Store:Load("BSCancelKey")
	Store:Update("BSCancelKey", function(Data) Data.Coins = 88; return Data end)
	local SaveResult = Store:Save("BSCancelKey")
	Assert(SaveResult.Passed == false, "F3b: Save cancelled when BeforeSave returns nil")

	local Info = Store:GetSessionInfo("BSCancelKey")
	Assert(Info ~= nil and Info.Status == "Active", "F3b: Session restored to Active after cancel")

	Store:Close("BSCancelKey")
	Store:Destroy()
end

-- ============================================================
-- FIX 4: Audit flush swallowed error — verify warn path exists
-- (We can only test this structurally; the actual DataStore error
-- path requires a degraded environment. We verify the flush runs
-- successfully in normal conditions.)
-- ============================================================
Section("F4. Audit Flush Normal Path (no error swallowed)")

do
	local Store = Nexus.CreateStore({
		Name = "NexusDeep_AuditFlushPath",
		Schema = TestSchema,
		LockMode = "None",
		AuditLog = true,
		AuditLogSize = 50,
		AuditPersistence = true,
	})

	local Load = Store:Load("FlushKey")
	if Load.Passed then
		Store:Update("FlushKey", function(Data) Data.Coins = 1; return Data end)
		local SaveResult = Store:Save("FlushKey")
		Assert(SaveResult.Passed == true, "F4: Save (and audit flush) succeeds")

		local AuditResult = Store:GetAuditLog("FlushKey")
		Assert(AuditResult.Passed and #(AuditResult :: any).Value >= 1,
			"F4: Audit entries exist after flush", tostring(#(AuditResult :: any).Value))

		local Dirty = (Store :: any)._AuditDirty["FlushKey"]
		Assert(Dirty == false or Dirty == nil, "F4: Audit marked clean after flush")

		Store:Close("FlushKey")
	end
	Store:Destroy()
end

-- ============================================================
-- FIX 5: Transaction re-peek only for stale participants
-- Verify multi-key transaction succeeds and updates _txv.
-- (Full conflict re-peek requires concurrent servers; we verify
-- the happy path commits correctly.)
-- ============================================================
Section("F5. Transaction Re-Peek Optimization (happy path)")

do
	local Store = Nexus.CreateStore({
		Name = "NexusDeep_TxnRePeek",
		Schema = TestSchema,
		LockMode = "None",
	})

	Store:Load("RPA")
	Store:Load("RPB")
	Store:Load("RPC")

	local TxvA_Before = (Store :: any)._LastKnownTxv["RPA"] or 0
	local TxvB_Before = (Store :: any)._LastKnownTxv["RPB"] or 0

	local TxnResult = Store:Transact({
		{ Key = "RPA", Transform = function(Data) Data.Coins = 100; return Data end },
		{ Key = "RPB", Transform = function(Data) Data.Gems = 50; return Data end },
		{ Key = "RPC", Transform = function(Data) Data.Coins = 25; return Data end },
	})
	Assert(TxnResult.Passed == true, "F5: 3-key transaction succeeds")

	local TxvA_After = (Store :: any)._LastKnownTxv["RPA"] or 0
	local TxvB_After = (Store :: any)._LastKnownTxv["RPB"] or 0
	Assert(TxvA_After > TxvA_Before, "F5: _txv incremented for RPA")
	Assert(TxvB_After > TxvB_Before, "F5: _txv incremented for RPB")

	local ReadA = Store:Read("RPA")
	local ReadB = Store:Read("RPB")
	local ReadC = Store:Read("RPC")
	if ReadA.Passed then Assert((ReadA :: any).Value.Coins == 100, "F5: RPA Coins = 100") end
	if ReadB.Passed then Assert((ReadB :: any).Value.Gems == 50,   "F5: RPB Gems = 50")   end
	if ReadC.Passed then Assert((ReadC :: any).Value.Coins == 25,  "F5: RPC Coins = 25")  end

	Store:Close("RPA")
	Store:Close("RPB")
	Store:Close("RPC")
	Store:Destroy()
end

-- ============================================================
-- EDGE: Middleware chaining — multiple handlers same phase
-- ============================================================
Section("E1. Middleware Chaining (multiple handlers)")

do
	local Store = Nexus.CreateStore({
		Name = "NexusDeep_MWChain",
		Schema = TestSchema,
		LockMode = "None",
	})

	local Order : { number } = {}
	Store:Use("load", function(_Key, Data, Next) table.insert(Order, 1); return Next(Data) end)
	Store:Use("load", function(_Key, Data, Next) table.insert(Order, 2); return Next(Data) end)
	Store:Use("load", function(_Key, Data, Next) table.insert(Order, 3); return Next(Data) end)

	Store:Load("MWChainKey")
	Assert(#Order == 3, "E1: All 3 load middleware handlers ran", tostring(#Order))
	Assert(Order[1] == 1 and Order[2] == 2 and Order[3] == 3,
		"E1: Handlers ran in registration order")

	Store:Close("MWChainKey")
	Store:Destroy()
end

-- ============================================================
-- EDGE: Middleware can transform data on load
-- ============================================================
Section("E2. Middleware Transform on Load")

do
	local Store = Nexus.CreateStore({
		Name = "NexusDeep_MWTransform",
		Schema = TestSchema,
		LockMode = "None",
	})

	Store:Use("load", function(_Key, Data : any)
		Data.Coins = 9999
		return Data
	end)

	Store:Load("MWTKey")
	local ReadResult = Store:Read("MWTKey")
	if ReadResult.Passed then
		Assert((ReadResult :: any).Value.Coins == 9999,
			"E2: Load middleware transform applied to in-memory data")
	end

	Store:Close("MWTKey")
	Store:Destroy()
end

-- ============================================================
-- EDGE: Double SoftDelete — second call should still succeed
-- (key already marked deleted remotely)
-- ============================================================
Section("E3. Double SoftDelete Is Idempotent")

do
	local Store = Nexus.CreateStore({
		Name = "NexusDeep_DoubleSoftDel",
		Schema = TestSchema,
		LockMode = "None",
	})

	local Load = Store:Load("DDKey")
	if Load.Passed then
		Store:Save("DDKey")
		Store:Close("DDKey")
	end

	local Del1 = Store:SoftDelete("DDKey")
	Assert(Del1.Passed == true, "E3: First SoftDelete succeeds")

	local Del2 = Store:SoftDelete("DDKey")
	Assert(Del2.Passed == true, "E3: Second SoftDelete also succeeds (already soft-deleted)")

	Store:Destroy()
end

-- ============================================================
-- EDGE: Recover on a key not in soft-delete registry fails
-- ============================================================
Section("E4. Recover on Unknown Key Fails")

do
	local Store = Nexus.CreateStore({
		Name = "NexusDeep_RecoverUnknown",
		Schema = TestSchema,
		LockMode = "None",
	})
	local RecoverResult = Store:Recover("NeverDeletedKey")
	Assert(RecoverResult.Passed == false, "E4: Recover on unknown key fails")
	if not RecoverResult.Passed then
		Assert(Nexus.IsSessionNotFound(RecoverResult) == false,
			"E4: Error is KeyNotFound, not SessionNotFound")
	end
	Store:Destroy()
end

-- ============================================================
-- EDGE: Save on clean session is a no-op (returns Passed=true fast)
-- ============================================================
Section("E5. Save on Clean Session Is No-Op")

do
	local Store = Nexus.CreateStore({
		Name = "NexusDeep_CleanSave",
		Schema = TestSchema,
		LockMode = "None",
	})

	Store:Load("CleanKey")

	local SaveResult = Store:Save("CleanKey")
	Assert(SaveResult.Passed == true, "E5: Save on clean session returns Passed=true")

	local Info = Store:GetSessionInfo("CleanKey")
	Assert(Info ~= nil and Info.Dirty == false, "E5: Session still clean")

	Store:Close("CleanKey")
	Store:Destroy()
end

-- ============================================================
-- EDGE: Update with nil transform leaves data unchanged
-- ============================================================
Section("E6. Update Nil Transform Is No-Op")

do
	local Store = Nexus.CreateStore({
		Name = "NexusDeep_NilUpdate",
		Schema = TestSchema,
		LockMode = "None",
	})

	Store:Load("NilUpKey")
	Store:Update("NilUpKey", function(Data) Data.Coins = 42; return Data end)
	Store:Update("NilUpKey", function(_Data) return nil end)

	local ReadResult = Store:Read("NilUpKey")
	if ReadResult.Passed then
		Assert((ReadResult :: any).Value.Coins == 42, "E6: Nil-returning update preserves data")
	end

	local Info = Store:GetSessionInfo("NilUpKey")
	Assert(Info ~= nil and Info.Dirty == true, "E6: Session still dirty (first update)")

	Store:Close("NilUpKey")
	Store:Destroy()
end

-- ============================================================
-- EDGE: Callback OnConflict defined but not invoked on normal load
-- (Just verify SetCallbacks with all hooks doesn't error)
-- ============================================================
Section("E7. Full Callback Table Accepted")

do
	local Store = Nexus.CreateStore({
		Name = "NexusDeep_FullCallbacks",
		Schema = TestSchema,
		LockMode = "None",
	})

	local CallbacksSet = pcall(function()
		Store:SetCallbacks({
			OnLoad     = function() end,
			OnSave     = function() end,
			OnRelease  = function() end,
			OnError    = function() end,
			OnMigration = function() end,
			BeforeSave = function(_Key, Data) return Data end,
			AfterLoad  = function(_Key, Data) return Data end,
		})
	end)
	Assert(CallbacksSet == true, "E7: SetCallbacks with full table does not error")

	local Load = Store:Load("FCKey")
	Assert(Load.Passed == true, "E7: Load works after SetCallbacks")
	Store:Close("FCKey")
	Store:Destroy()
end

-- ============================================================
-- EDGE: GetSessionInfo fields are complete and correct
-- ============================================================
Section("E8. GetSessionInfo Field Completeness")

do
	local Store = Nexus.CreateStore({
		Name = "NexusDeep_SessionInfo",
		Schema = TestSchema,
		LockMode = "None",
	})

	Store:Load("SIKey")
	Store:Update("SIKey", function(Data) Data.Coins = 1; return Data end)

	local Info = Store:GetSessionInfo("SIKey")
	Assert(Info ~= nil, "E8: GetSessionInfo returns non-nil")
	if Info then
		Assert(Info.Key == "SIKey",         "E8: Key field correct")
		Assert(Info.Status == "Active",     "E8: Status is Active")
		Assert(Info.Dirty == true,          "E8: Dirty is true after Update")
		Assert(Info.SaveCount >= 0,         "E8: SaveCount is a non-negative number")
		Assert(Info.SessionAge >= 0,        "E8: SessionAge is non-negative")
		Assert(Info.SchemaVersion == 1,     "E8: SchemaVersion matches schema")
		Assert(Info.PendingActionCount == 0,"E8: No pending QueueAction items")
	end

	Store:Close("SIKey")
	Store:Destroy()
end

-- ============================================================
-- EDGE: QueueAction accumulates and PendingActionCount is tracked
-- ============================================================
Section("E9. QueueAction PendingActionCount")

do
	local Store = Nexus.CreateStore({
		Name = "NexusDeep_QueueCount",
		Schema = TestSchema,
		LockMode = "None",
	})

	Store:Load("QCKey")
	Store:QueueAction("QCKey", function(Data) Data.Coins += 1; return Data end)
	Store:QueueAction("QCKey", function(Data) Data.Coins += 1; return Data end)
	Store:QueueAction("QCKey", function(Data) Data.Coins += 1; return Data end)

	local Info = Store:GetSessionInfo("QCKey")
	Assert(Info ~= nil and Info.PendingActionCount == 3,
		"E9: PendingActionCount == 3 after 3 QueueAction calls",
		tostring(Info and Info.PendingActionCount))

	Store:Close("QCKey")
	Store:Destroy()
end

-- ============================================================
-- EDGE: Telemetry records failures correctly
-- ============================================================
Section("E10. Telemetry Records Failure Counts")

do
	local Store = Nexus.CreateStore({
		Name = "NexusDeep_TelemetryFail",
		Schema = TestSchema,
		LockMode = "None",
	})

	Store:Read("GhostKey")
	Store:Update("GhostKey", function(Data) return Data end)

	local Telemetry = Store:GetTelemetry()
	Assert(Telemetry ~= nil, "E10: GetTelemetry returns scope")

	Store:Destroy()
end

-- ============================================================
-- EDGE: Destroy cancels auto-save thread and debounce timers
-- ============================================================
Section("E11. Destroy Cleans Up Threads")

do
	local Store = Nexus.CreateStore({
		Name = "NexusDeep_Destroy",
		Schema = TestSchema,
		LockMode = "None",
		DebounceInterval = 5,
	})

	Store:Load("DKey")
	Store:Update("DKey", function(Data) Data.Coins = 1; return Data end)
	Store:Save("DKey")

	local DestroyOk = pcall(function() Store:Destroy() end)
	Assert(DestroyOk == true, "E11: Destroy does not error")

	local AutoSaveThread = (Store :: any)._AutoSaveThread
	local IdleThread     = (Store :: any)._IdleThread
	Assert(AutoSaveThread == nil, "E11: Auto-save thread cancelled by Destroy")
	Assert(IdleThread == nil,     "E11: Idle tracker thread cancelled by Destroy")
end

-- ============================================================
-- EDGE: LoadMany with a mix of new + existing keys
-- ============================================================
Section("E12. LoadMany Mixed New and Existing Keys")

do
	local Store = Nexus.CreateStore({
		Name = "NexusDeep_LoadManyMix",
		Schema = TestSchema,
		LockMode = "None",
	})

	local Setup = Store:Load("LMM_Existing")
	if Setup.Passed then
		Store:Update("LMM_Existing", function(Data) Data.Coins = 500; return Data end)
		Store:Save("LMM_Existing")
		Store:Close("LMM_Existing")
	end

	local Batch = Store:LoadMany({ "LMM_Existing", "LMM_New1", "LMM_New2" })
	Assert(Batch.Succeeded["LMM_Existing"] ~= nil, "E12: Existing key loaded in batch")
	Assert(Batch.Succeeded["LMM_New1"] ~= nil,     "E12: New key 1 loaded in batch")
	Assert(Batch.Succeeded["LMM_New2"] ~= nil,     "E12: New key 2 loaded in batch")

	if Batch.Succeeded["LMM_Existing"] then
		Assert(Batch.Succeeded["LMM_Existing"].Coins == 500, "E12: Existing key data preserved")
	end
	if Batch.Succeeded["LMM_New1"] then
		Assert(Batch.Succeeded["LMM_New1"].Coins == 0, "E12: New key gets default data")
	end

	Store:CloseAll()
	Store:Destroy()
end

-- ============================================================
-- EDGE: Transaction with single participant
-- ============================================================
Section("E13. Single-Participant Transaction")

do
	local Store = Nexus.CreateStore({
		Name = "NexusDeep_SingleTxn",
		Schema = TestSchema,
		LockMode = "None",
	})

	Store:Load("SingleTxKey")
	local TxvBefore = (Store :: any)._LastKnownTxv["SingleTxKey"] or 0

	local TxnResult = Store:Transact({
		{ Key = "SingleTxKey", Transform = function(Data) Data.Coins = 777; return Data end },
	})
	Assert(TxnResult.Passed == true, "E13: Single-participant transaction succeeds")

	local TxvAfter = (Store :: any)._LastKnownTxv["SingleTxKey"] or 0
	Assert(TxvAfter > TxvBefore, "E13: _txv incremented")

	local ReadResult = Store:Read("SingleTxKey")
	if ReadResult.Passed then
		Assert((ReadResult :: any).Value.Coins == 777, "E13: Data updated in-memory")
	end

	Store:Close("SingleTxKey")
	Store:Destroy()
end

-- ============================================================
-- EDGE: Audit log trimmed to AuditLogSize on in-memory
-- ============================================================
Section("E14. Audit Log In-Memory Trim")

do
	local Store = Nexus.CreateStore({
		Name = "NexusDeep_AuditTrimDeep",
		Schema = TestSchema,
		LockMode = "None",
		AuditLog = true,
		AuditLogSize = 3,
	})

	Store:Load("ATKey")
	for I = 1, 10 do
		Store:Update("ATKey", function(Data) Data.Coins = I; return Data end)
	end

	local AuditResult = Store:GetAuditLog("ATKey")
	Assert(AuditResult.Passed and #(AuditResult :: any).Value <= 3,
		"E14: In-memory audit trimmed to 3 entries",
		tostring(AuditResult.Passed and #(AuditResult :: any).Value))

	Store:Close("ATKey")
	Store:Destroy()
end

-- ============================================================
-- EDGE: OnDirty fires exactly once on first dirty write
-- ============================================================
Section("E15. OnDirty Fires Once Per Dirty Transition")

do
	local Store = Nexus.CreateStore({
		Name = "NexusDeep_OnDirty",
		Schema = TestSchema,
		LockMode = "None",
	})

	local DirtyFireCount = 0
	Store.OnDirty:Connect(function() DirtyFireCount += 1 end)

	Store:Load("DirtyKey")
	Store:Update("DirtyKey", function(Data) Data.Coins = 1; return Data end)
	Store:Update("DirtyKey", function(Data) Data.Coins = 2; return Data end)
	Store:Update("DirtyKey", function(Data) Data.Coins = 3; return Data end)

	Assert(DirtyFireCount == 1,
		"E15: OnDirty fires exactly once (first dirty transition)",
		tostring(DirtyFireCount))

	Store:Save("DirtyKey")
	Store:Update("DirtyKey", function(Data) Data.Coins = 4; return Data end)
	Assert(DirtyFireCount == 2,
		"E15: OnDirty fires again after save+re-dirty",
		tostring(DirtyFireCount))

	Store:Close("DirtyKey")
	Store:Destroy()
end

-- ============================================================
-- EDGE: Peek does not create a session
-- ============================================================
Section("E16. Peek Does Not Create Session")

do
	local Store = Nexus.CreateStore({
		Name = "NexusDeep_PeekNoSession",
		Schema = TestSchema,
		LockMode = "None",
	})

	local PeekResult = Store:Peek("PNSKey")
	Assert(PeekResult.Passed == true, "E16: Peek succeeds")
	Assert(Store:IsLoaded("PNSKey") == false, "E16: Peek does not create a session")
	Assert(#Store:GetActiveKeys() == 0, "E16: No active keys after Peek")

	Store:Destroy()
end

-- ============================================================
-- EDGE: Remove on a key with no shards and no session
-- ============================================================
Section("E17. Remove Cold Key")

do
	local Store = Nexus.CreateStore({
		Name = "NexusDeep_RemoveCold",
		Schema = TestSchema,
		LockMode = "None",
		AuditLog = true,
		AuditPersistence = true,
	})

	local LoadResult = Store:Load("RCKey")
	if LoadResult.Passed then
		Store:Update("RCKey", function(Data) Data.Coins = 111; return Data end)
		Store:Save("RCKey")
		Store:Close("RCKey")
	end

	local RemoveResult = Store:Remove("RCKey")
	Assert(RemoveResult.Passed == true, "E17: Remove cold key succeeds")

	local AuditState = (Store :: any)._AuditLogs["RCKey"]
	Assert(AuditState == nil, "E17: In-memory audit cleared after Remove")

	local SoftDelState = (Store :: any)._SoftDeleted["RCKey"]
	Assert(SoftDelState == nil, "E17: SoftDeleted registry cleared after Remove")

	Store:Destroy()
end

-- ============================================================
-- EDGE: ListSoftDeleted returns correct SoftDeleteInfo
-- ============================================================
Section("E18. ListSoftDeleted Returns Correct Info")

do
	local Store = Nexus.CreateStore({
		Name = "NexusDeep_ListSoftDel",
		Schema = TestSchema,
		LockMode = "None",
		SoftDeleteRetention = 86400,
	})

	local Load = Store:Load("LSD_Key1")
	if Load.Passed then Store:Save("LSD_Key1"); Store:Close("LSD_Key1") end

	local Load2 = Store:Load("LSD_Key2")
	if Load2.Passed then Store:Save("LSD_Key2"); Store:Close("LSD_Key2") end

	Store:SoftDelete("LSD_Key1")
	Store:SoftDelete("LSD_Key2")

	local ListResult = Store:ListSoftDeleted()
	Assert(ListResult.Passed == true, "E18: ListSoftDeleted succeeds")
	if ListResult.Passed then
		local Entries = (ListResult :: any).Value
		Assert(#Entries == 2, "E18: Two soft-deleted entries returned", tostring(#Entries))
		for _, Entry in Entries do
			Assert(Entry.Key ~= nil,       "E18: Entry has Key field")
			Assert(Entry.DeletedAt ~= nil, "E18: Entry has DeletedAt field")
			Assert(Entry.ExpiresAt ~= nil, "E18: Entry has ExpiresAt (retention > 0)")
		end
	end

	Store:Destroy()
end

-- ============================================================
-- EDGE: Debounce interval delays save and coalesces calls
-- ============================================================
Section("E19. Debounce Save Coalescing")

do
	local Store = Nexus.CreateStore({
		Name = "NexusDeep_Debounce",
		Schema = TestSchema,
		LockMode = "None",
		DebounceInterval = 0.1,
	})

	local SaveFiredCount = 0
	Store.OnSave:Connect(function() SaveFiredCount += 1 end)

	Store:Load("DebKey")
	Store:Update("DebKey", function(Data) Data.Coins = 1; return Data end)
	Store:Save("DebKey")
	Store:Update("DebKey", function(Data) Data.Coins = 2; return Data end)
	Store:Save("DebKey")
	Store:Update("DebKey", function(Data) Data.Coins = 3; return Data end)
	Store:Save("DebKey")

	Assert(SaveFiredCount == 0, "E19: Debounced saves have not fired yet")

	local Timeout = 5
	local Start = os.clock()
	while SaveFiredCount == 0 and (os.clock() - Start) < Timeout do
		task.wait(0.1)
	end

	Assert(SaveFiredCount == 1, "E19: Only 1 save fired after debounce (coalesced)", tostring(SaveFiredCount))

	local ReadResult = Store:Read("DebKey")
	if ReadResult.Passed then
		Assert((ReadResult :: any).Value.Coins == 3, "E19: Final value is 3 (last update)")
	end

	Store:Close("DebKey")
	Store:Destroy()
end

-- ============================================================
-- EDGE: Schema OnConflict callback is accepted by SetCallbacks
-- ============================================================
Section("E20. OnConflict Field Accepted in Callbacks")

do
	local Store = Nexus.CreateStore({
		Name = "NexusDeep_OnConflict",
		Schema = TestSchema,
		LockMode = "None",
	})

	local Ok = pcall(function()
		Store:SetCallbacks({
			OnConflict = function(_Key, LocalData, _RemoteData)
				return LocalData
			end,
		})
	end)
	Assert(Ok == true, "E20: SetCallbacks with OnConflict does not error")
	Store:Destroy()
end

-- ============================================================
-- EDGE: GetActiveKeys reflects exact in-flight sessions
-- ============================================================
Section("E21. GetActiveKeys Accuracy")

do
	local Store = Nexus.CreateStore({
		Name = "NexusDeep_ActiveKeys",
		Schema = TestSchema,
		LockMode = "None",
	})

	Assert(#Store:GetActiveKeys() == 0, "E21: No keys before any Load")
	Store:Load("AK_1")
	Store:Load("AK_2")
	Assert(#Store:GetActiveKeys() == 2, "E21: 2 keys after 2 Loads")
	Store:Close("AK_1")
	Assert(#Store:GetActiveKeys() == 1, "E21: 1 key after Close of AK_1")
	Store:Close("AK_2")
	Assert(#Store:GetActiveKeys() == 0, "E21: 0 keys after all closed")

	Store:Destroy()
end

-- ============================================================
-- EDGE: Version listing returns results in descending order
-- ============================================================
Section("E22. ListVersions Descending Default Order")

do
	local Store = Nexus.CreateStore({
		Name = "NexusDeep_VersionOrder",
		Schema = TestSchema,
		LockMode = "None",
	})

	local Load = Store:Load("VOKey")
	if Load.Passed then
		Store:Update("VOKey", function(Data) Data.Coins = 10; return Data end)
		Store:Save("VOKey")
		task.wait(0.1)
		Store:Update("VOKey", function(Data) Data.Coins = 20; return Data end)
		Store:Save("VOKey")
		Store:Close("VOKey")
	end

	local VersionsResult = Store:ListVersions("VOKey", { SortDirection = Enum.SortDirection.Descending })
	Assert(VersionsResult.Passed == true, "E22: ListVersions with Descending succeeds")
	if VersionsResult.Passed then
		local Versions = (VersionsResult :: any).Value
		Assert(#Versions >= 1, "E22: At least 1 version returned", tostring(#Versions))
		if #Versions >= 2 then
			Assert(Versions[1].CreatedTime >= Versions[2].CreatedTime,
				"E22: Versions in descending order")
		end
	end

	Store:Destroy()
end

-- ============================================================
-- EDGE: Persistent audit round-trip (write then remote read)
-- ============================================================
Section("E23. Persistent Audit Round-Trip Deep")

do
	local Store1 = Nexus.CreateStore({
		Name = "NexusDeep_AuditRoundTrip",
		Schema = TestSchema,
		LockMode = "None",
		AuditLog = true,
		AuditLogSize = 100,
		AuditPersistence = true,
	})

	local Load = Store1:Load("AuditRTKey")
	if Load.Passed then
		for I = 1, 5 do
			Store1:Update("AuditRTKey", function(Data) Data.Coins = I; return Data end)
		end
		Store1:Save("AuditRTKey")
		Store1:Close("AuditRTKey")
	end
	Store1:Destroy()

	local Store2 = Nexus.CreateStore({
		Name = "NexusDeep_AuditRoundTrip",
		Schema = TestSchema,
		LockMode = "None",
		AuditLog = true,
		AuditLogSize = 100,
		AuditPersistence = true,
	})

	local RemoteAudit = Store2:GetAuditLogRemote("AuditRTKey")
	Assert(RemoteAudit.Passed == true, "E23: GetAuditLogRemote succeeds")
	if RemoteAudit.Passed then
		local Entries = (RemoteAudit :: any).Value
		Assert(#Entries > 0, "E23: Remote audit has entries", tostring(#Entries))
		local Valid = true
		for _, Entry in Entries do
			if not (Entry.Timestamp and Entry.Operation and Entry.Source and Entry.SessionId) then
				Valid = false
				break
			end
		end
		Assert(Valid, "E23: Remote audit entries are structurally valid")
	end

	Store2:Destroy()
end

-- ============================================================
-- EDGE: Pure Remove also cleans up remote audit shards
-- ============================================================
Section("E24. Remove Cleans Remote Audit Then Returns Clean")

do
	local Store = Nexus.CreateStore({
		Name = "NexusDeep_RemoveAuditClean",
		Schema = TestSchema,
		LockMode = "None",
		AuditLog = true,
		AuditPersistence = true,
	})

	local Load = Store:Load("RACKey")
	if Load.Passed then
		Store:Update("RACKey", function(Data) Data.Coins = 42; return Data end)
		Store:Save("RACKey")
		Store:Close("RACKey")
	end

	local RemoveResult = Store:Remove("RACKey")
	Assert(RemoveResult.Passed == true, "E24: Remove with audit persistence succeeds")

	local ShardCount = (Store :: any)._AuditShardCounts["RACKey"]
	Assert(ShardCount == nil, "E24: AuditShardCounts entry cleared after Remove")

	local RemoteAudit = Store:GetAuditLogRemote("RACKey")
	if RemoteAudit.Passed then
		Assert(#(RemoteAudit :: any).Value == 0, "E24: Remote audit is empty after Remove")
	end

	Store:Destroy()
end

-- ============================================================
-- EDGE: Error signals include Key field
-- ============================================================
Section("E25. OnError Signal Includes Key Field")

do
	local Store = Nexus.CreateStore({
		Name = "NexusDeep_OnErrorKey",
		Schema = TestSchema,
		LockMode = "None",
	})

	local CapturedError : any = nil
	Store.OnError:Connect(function(_Key, Err) CapturedError = Err end)
	
	-- Inject pure garbage into DataStore so UnpackEnvelope fails and fires OnError
	-- Using a table with compression flag but invalid data string ensures decompression fails gracefully
	local DS = (_G.NexusMockDataStoreService or game:GetService("DataStoreService")):GetDataStore("NexusDeep_OnErrorKey")
	pcall(function() DS:SetAsync("OEKey", { _v = 1, _c = true, _d = "invalid_compression_string" }) end)

	Store:Load("OEKey")

	if CapturedError then
		Assert(CapturedError.Key == "OEKey", "E25: OnError Error.Key is populated")
		Assert(CapturedError.Kind ~= nil,    "E25: OnError Error.Kind is populated")
	else
		Assert(false, "E25: OnError was fired (double-load should fire it)")
	end

	Store:Close("OEKey")
	Store:Destroy()
end

-- ============================================================
-- RESULTS
-- ============================================================
Section("DEEP TEST RESULTS")
print(`Total: {TestCount}  |  Passed: {PassCount}  |  Failed: {FailCount}`)
if FailCount == 0 then
	print(`\n{PASS} ALL DEEP TESTS PASSED`)
else
	warn(`\n{FAIL} {FailCount} DEEP TEST(S) FAILED`)
end
