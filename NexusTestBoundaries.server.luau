--!strict
local ReplicatedStorage = game:GetService("ReplicatedStorage")
local NexusTestHelpers = require(ReplicatedStorage.Nexus.NexusTestHelpers)
local Nexus = NexusTestHelpers.Nexus
local Suite = NexusTestHelpers.CreateSuite()
local Assert = Suite.Assert
local Section = Suite.Section

local ValidatedSchema = {
	Version = 1,
	Default = { Coins = 0 },
	Validate = function(Data : any) return true, nil end
}

local MigratableSchema = {
	Version = 2,
	Default = { Coins = 0, Level = 1 },
	Migrations = {
		{ Version = 2, Up = function(Data : any)
			Data.Level = Data.Level or 1
			return Data
		end}
	}
}

Section("B-SCHEMA: Validations and Migrations")
do
	local Harness = NexusTestHelpers.TestHarness.New()
	local Store1 = Harness:CreateStore({ Name = "Boundaries_Schema", Schema = ValidatedSchema })
	
	local LoadResult = Store1:Load("SchemaKey")
	Assert(LoadResult.Passed, "Loaded V1 schema")
	if LoadResult.Passed then
		Store1:Update("SchemaKey", function(d) d.Coins = 100; return d end)
		Store1:Save("SchemaKey")
		Store1:Close("SchemaKey")
	end
	
	local Store2 = Harness:CreateStore({ Name = "Boundaries_Schema", Schema = MigratableSchema })
	local LoadR2 = Store2:Load("SchemaKey")
	Assert(LoadR2.Passed, "Loaded V2 schema with migration")
	if LoadR2.Passed then
		local d = (LoadR2 :: any).Value
		Assert(d.Coins == 100, "V1 data retained")
		Assert(d.Level == 1, "V2 migration default applied")
	end
	
	Harness:Teardown()
end

Section("B-VALIDATION: Non-Serializable Types")
do
	local Harness = NexusTestHelpers.TestHarness.New()
	local Store = Harness:CreateStore({ Name = "Boundaries_Val", Schema = ValidatedSchema })
	
	local LoadResult = Store:Load("ValKey")
	if LoadResult.Passed then
		local BadUpdate = Store:Update("ValKey", function(d)
			d.Coins = Vector3.new(1,2,3) -- vector3 shouldn't be json safe natively (though Datastore handles some)
			return d
		end)
		-- Nexus.IsJsonSafe verifies standard structure. 
		local IsSafe = Nexus.IsJsonSafe({Coins = Vector3.new()})
		Assert(IsSafe == false, "[expected] Vector3 is rejected by IsJsonSafe")
	end
	
	Harness:Teardown()
end

Section("B-SIZE: Compression Boundaries")
do
	local Harness = NexusTestHelpers.TestHarness.New()
	local Store = Harness:CreateStore({ Name = "Boundaries_Size", Schema = ValidatedSchema })
	
	local LoadResult = Store:Load("SizeKey")
	if LoadResult.Passed then
		local largeString = string.rep("A", 4096)
		Store:Update("SizeKey", function(d) d.Payload = largeString; return d end)
		local SaveR = Store:Save("SizeKey")
		Assert(SaveR.Passed, "Successfully saved exactly compression threshold limit string")
	end
	
	Harness:Teardown()
end

Suite.Summary()
