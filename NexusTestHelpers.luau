--!strict
local ReplicatedStorage = game:GetService("ReplicatedStorage")
local Nexus = require(ReplicatedStorage.Nexus)
local MockDataStore = require(ReplicatedStorage.Nexus.MockDataStore)

local Helpers = {}

local PASS = "✅"
local FAIL = "❌"

_G.NexusTestTally = _G.NexusTestTally or { TestCount = 0, PassCount = 0, FailCount = 0, SectionCount = 0 }

function Helpers.Assert(Condition : boolean, TestName : string, Detail : string?)
	_G.NexusTestTally.TestCount += 1
	if Condition then
		_G.NexusTestTally.PassCount += 1
		print(`{PASS} {TestName}`)
	else
		_G.NexusTestTally.FailCount += 1
		local prefix = if string.find(TestName:lower(), "expected") then "[expected] " else ""
		warn(`{prefix}{FAIL} {TestName}{if Detail then " · " .. Detail else ""}`)
	end
end

function Helpers.Section(Title : string)
	_G.NexusTestTally.SectionCount += 1
	print(`\n{string.rep("=", 60)}`)
	print(`  {Title}`)
	print(`{string.rep("=", 60)}`)
end

function Helpers.CombinedSummary()
	print(`\n{string.rep("=", 60)}`)
	print(`Total: {_G.NexusTestTally.PassCount}/{_G.NexusTestTally.TestCount} passed across {_G.NexusTestTally.SectionCount} sections`)
	print(`{string.rep("=", 60)}`)
end

function Helpers.CreateSuite()
	local Tally = { TestCount = 0, PassCount = 0, FailCount = 0, SectionCount = 0 }
	local Suite = {}

	Suite.Assert = function(Condition : boolean, TestName : string, Detail : string?)
		Tally.TestCount += 1
		if Condition then
			Tally.PassCount += 1
			print(`{PASS} {TestName}`)
		else
			Tally.FailCount += 1
			local Prefix = if string.find(TestName:lower(), "expected") then "[expected] " else ""
			warn(`{Prefix}{FAIL} {TestName}{if Detail then " · " .. Detail else ""}`)
		end
	end

	Suite.Section = function(Title : string)
		Tally.SectionCount += 1
		print(`\n{string.rep("=", 60)}`)
		print(`  {Title}`)
		print(`{string.rep("=", 60)}`)
	end

	Suite.Summary = function()
		print(`\n{string.rep("=", 60)}`)
		print(`Total: {Tally.PassCount}/{Tally.TestCount} passed across {Tally.SectionCount} sections`)
		print(`{string.rep("=", 60)}`)
	end

	return Suite
end

local TestHarness = {}
TestHarness.__index = TestHarness

function TestHarness.New()
	local self = setmetatable({}, TestHarness)
	self.Stores = {}
	self.InitialStoreCount = Nexus.Lifecycle.GetRegisteredCount()
	MockDataStore.Inject()
	return self
end

function TestHarness:CreateStore(Config : any?)
	local Store = Nexus.CreateStore(Config)
	table.insert(self.Stores, Store)
	return Store
end

function TestHarness:CreateLeaderboard(Config : any?)
	local LB = Nexus.CreateLeaderboard(Config)
	table.insert(self.Stores, LB)
	return LB
end

function TestHarness:CreatePlayerStore(Config : any?)
	local PS = Nexus.CreatePlayerStore(Config)
	table.insert(self.Stores, PS)
	return PS
end

function TestHarness:Teardown()
	for _, Store in self.Stores do
		pcall(function() Store:Destroy() end)
	end
	table.clear(self.Stores)
	MockDataStore.Unject()
end

Helpers.TestHarness = TestHarness
Helpers.MockDataStore = MockDataStore
Helpers.Nexus = Nexus

return Helpers
