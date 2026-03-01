--!strict

local Errors = require(script.Parent.Errors)
local Types = require(script.Parent.Types)
local Retry = require(script.Parent.Retry)

type Result<Value> = Types.Result<Value>

local Shard = {}

local DEFAULT_SHARD_THRESHOLD = 3_500_000

local SHARD_CHUNK_SIZE = 3_500_000

function Shard.NeedsSharding(
	CompressedPayload : string,
	ShardThreshold : number?
) : boolean
	local Threshold = ShardThreshold or DEFAULT_SHARD_THRESHOLD
	if Threshold == 0 then
		return false
	end
	return #CompressedPayload > Threshold
end

function Shard.Split(
	CompressedPayload : string,
	ShardThreshold : number?
) : { string }
	local ChunkSize = ShardThreshold or SHARD_CHUNK_SIZE
	if ChunkSize == 0 then
		ChunkSize = SHARD_CHUNK_SIZE
	end
	local PayloadLength = #CompressedPayload
	local ChunkCount = math.ceil(PayloadLength / ChunkSize)
	local Chunks : { string } = {}

	for Index = 1, ChunkCount do
		local StartByte = (Index - 1) * ChunkSize + 1
		local EndByte = math.min(Index * ChunkSize, PayloadLength)
		table.insert(Chunks, string.sub(CompressedPayload, StartByte, EndByte))
	end

	return Chunks
end

function Shard.ShardKey(BaseKey : string, ShardIndex : number) : string
	return `{BaseKey}_shard_{ShardIndex}`
end

function Shard.WriteShards(
	DataStore : DataStore,
	BaseKey : string,
	Chunks : { string },
	MaxRetries : number?
) : Result<boolean>
	local RetryCount = MaxRetries or 3

	for Index, Chunk in Chunks do
		local ShardKeyName = Shard.ShardKey(BaseKey, Index)
		local WriteResult = Retry.WrapPcall("DataStoreError", function()
			DataStore:SetAsync(ShardKeyName, Chunk)
			return true
		end, { MaxRetries = RetryCount, BudgetType = "SetIncrementAsync" } :: any)

		if not WriteResult.Passed then
			return Errors.Fail(Errors.New("DataStoreError",
				`Failed to write shard {Index}/{#Chunks} for key '{BaseKey}': `
				.. `{(WriteResult :: Types.ErrorResult).Error.Message}`
			))
		end
	end

	return Errors.Passed(true)
end

function Shard.ReadShards(
	DataStore : DataStore,
	BaseKey : string,
	ShardCount : number,
	MaxRetries : number?
) : Result<string>
	local RetryCount = MaxRetries or 3
	local Chunks : { string } = table.create(ShardCount) :: any

	for Index = 1, ShardCount do
		local ShardKeyName = Shard.ShardKey(BaseKey, Index)
		local ReadResult = Retry.WrapPcall("DataStoreError", function()
			return DataStore:GetAsync(ShardKeyName)
		end, { MaxRetries = RetryCount, BudgetType = "GetAsync" } :: any)

		if not ReadResult.Passed then
			return Errors.Fail(Errors.New("DataStoreError",
				`Failed to read shard {Index}/{ShardCount} for key '{BaseKey}': `
				.. `{(ReadResult :: Types.ErrorResult).Error.Message}`
			))
		end

		local ChunkData = (ReadResult :: Types.SuccessResult<unknown>).Value
		if typeof(ChunkData) ~= "string" then
			return Errors.Fail(Errors.New("DataStoreError",
				`Shard {Index} for key '{BaseKey}' has unexpected type '{typeof(ChunkData)}', expected string`
			))
		end

		Chunks[Index] = ChunkData :: string
	end

	return Errors.Passed(table.concat(Chunks))
end

function Shard.CleanupOrphanedShards(
	DataStore : DataStore,
	BaseKey : string,
	OldShardCount : number,
	NewShardCount : number
) : ()
	if OldShardCount <= NewShardCount then
		return
	end
	for Index = NewShardCount + 1, OldShardCount do
		local ShardKeyName = Shard.ShardKey(BaseKey, Index)
		pcall(function()
			DataStore:RemoveAsync(ShardKeyName)
		end)
	end
end

function Shard.DeleteAllShards(
	DataStore : DataStore,
	BaseKey : string,
	ShardCount : number
) : ()
	for Index = 1, ShardCount do
		local ShardKeyName = Shard.ShardKey(BaseKey, Index)
		pcall(function()
			DataStore:RemoveAsync(ShardKeyName)
		end)
	end
end

return Shard
