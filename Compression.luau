--!strict

local HttpService = game:GetService("HttpService")
local EncodingService = game:GetService("EncodingService")

local Errors = require(script.Parent.Errors)
local Types = require(script.Parent.Types)

type Result<Value> = Types.Result<Value>

local Compression = {}

local DEFAULT_COMPRESSION_LEVEL = 3
local MIN_COMPRESSION_LEVEL = 1
local MAX_COMPRESSION_LEVEL = 22

local function ResolveLevel(Requested : number?) : number
	if Requested == nil then return DEFAULT_COMPRESSION_LEVEL end
	if typeof(Requested) ~= "number" or Requested ~= Requested then
		return DEFAULT_COMPRESSION_LEVEL
	end
	local Clamped = math.clamp(math.floor(Requested), MIN_COMPRESSION_LEVEL, MAX_COMPRESSION_LEVEL)
	return Clamped
end

function Compression.Compress(
	Data : { [string] : unknown },
	CompressionLevel : number?
) : Result<string>
	local EncodeSuccess, JsonString = pcall(HttpService.JSONEncode, HttpService, Data)
	if not EncodeSuccess then
		return Errors.Fail(Errors.New("CompressionError", `Failed to JSON-encode: {JsonString}`))
	end

	local Level = ResolveLevel(CompressionLevel)
	local InputBuffer = buffer.fromstring(JsonString :: string)

	local CompressSuccess, CompressedBuffer = pcall(function()
		return EncodingService:CompressBuffer(InputBuffer, Enum.CompressionAlgorithm.Zstd, Level)
	end)
	if not CompressSuccess then
		return Errors.Fail(Errors.New("CompressionError", `ZSTD compression failed: {CompressedBuffer}`))
	end

	local Base64Success, Base64Buffer = pcall(function()
		return EncodingService:Base64Encode(CompressedBuffer :: buffer)
	end)
	if not Base64Success then
		return Errors.Fail(Errors.New("CompressionError", `Base64 encoding failed: {Base64Buffer}`))
	end

	return Errors.Passed(buffer.tostring(Base64Buffer :: buffer))
end

function Compression.CompressString(
	JsonString : string,
	CompressionLevel : number?
) : Result<string>
	local Level = ResolveLevel(CompressionLevel)
	local InputBuffer = buffer.fromstring(JsonString)

	local CompressSuccess, CompressedBuffer = pcall(function()
		return EncodingService:CompressBuffer(InputBuffer, Enum.CompressionAlgorithm.Zstd, Level)
	end)
	if not CompressSuccess then
		return Errors.Fail(Errors.New("CompressionError", `ZSTD compression failed: {CompressedBuffer}`))
	end

	local Base64Success, Base64Buffer = pcall(function()
		return EncodingService:Base64Encode(CompressedBuffer :: buffer)
	end)
	if not Base64Success then
		return Errors.Fail(Errors.New("CompressionError", `Base64 encoding failed: {Base64Buffer}`))
	end

	return Errors.Passed(buffer.tostring(Base64Buffer :: buffer))
end

function Compression.Decompress(EncodedData : string) : Result<{ [string] : unknown }>
	return Compression._DecompressZstd(EncodedData)
end

function Compression.DecompressToString(EncodedData : string) : Result<string>
	local Base64DecodeSuccess, DecodedBuffer = pcall(function()
		return EncodingService:Base64Decode(buffer.fromstring(EncodedData))
	end)
	if not Base64DecodeSuccess then
		return Errors.Fail(Errors.New("CompressionError", `Base64 decode failed: {DecodedBuffer}`))
	end

	local DecompressSuccess, DecompressedBuffer = pcall(function()
		return EncodingService:DecompressBuffer(DecodedBuffer :: buffer, Enum.CompressionAlgorithm.Zstd)
	end)
	if not DecompressSuccess then
		return Errors.Fail(Errors.New("CompressionError", `ZSTD decompress failed: {DecompressedBuffer}`))
	end

	return Errors.Passed(buffer.tostring(DecompressedBuffer :: buffer))
end

function Compression.EstimateSize(Data : { [string] : unknown }) : number
	local EncodeSuccess, JsonString = pcall(HttpService.JSONEncode, HttpService, Data)
	if not EncodeSuccess then
		return 0
	end
	return #(JsonString :: string)
end

-- Single-encode helper: JSON-encodes once and compresses only if the result
-- exceeds Threshold. Returns (payload, isCompressed, jsonSize). This avoids
-- the double JSONEncode that happens when callers call EstimateSize then Compress.
function Compression.TryCompress(
	Data : { [string] : unknown },
	Threshold : number,
	CompressionLevel : number?
) : ({ [string] : unknown } | string, boolean, number)
	local EncodeOk, JsonString = pcall(HttpService.JSONEncode, HttpService, Data)
	if not EncodeOk then return Data, false, 0 end
	local Size = #(JsonString :: string)
	if Size <= Threshold then return Data, false, Size end

	local Level = ResolveLevel(CompressionLevel)
	local InputBuffer = buffer.fromstring(JsonString :: string)

	local CompressOk, CompressedBuffer = pcall(function()
		return EncodingService:CompressBuffer(InputBuffer, Enum.CompressionAlgorithm.Zstd, Level)
	end)
	if not CompressOk then return Data, false, Size end

	local Base64Ok, Base64Buffer = pcall(function()
		return EncodingService:Base64Encode(CompressedBuffer :: buffer)
	end)
	if not Base64Ok then return Data, false, Size end

	return buffer.tostring(Base64Buffer :: buffer), true, Size
end

function Compression._DecompressZstd(EncodedData : string) : Result<{ [string] : unknown }>
	local Base64DecodeSuccess, DecodedBuffer = pcall(function()
		return EncodingService:Base64Decode(buffer.fromstring(EncodedData))
	end)
	if not Base64DecodeSuccess then
		return Errors.Fail(Errors.New("CompressionError", `Base64 decode failed: {DecodedBuffer}`))
	end

	local DecompressSuccess, DecompressedBuffer = pcall(function()
		return EncodingService:DecompressBuffer(DecodedBuffer :: buffer, Enum.CompressionAlgorithm.Zstd)
	end)
	if not DecompressSuccess then
		return Errors.Fail(Errors.New("CompressionError", `ZSTD decompress failed: {DecompressedBuffer}`))
	end

	local JsonString = buffer.tostring(DecompressedBuffer :: buffer)
	local ParseSuccess, ParsedData = pcall(HttpService.JSONDecode, HttpService, JsonString)
	if not ParseSuccess then
		return Errors.Fail(Errors.New("CompressionError", `JSON decode failed: {ParsedData}`))
	end

	return Errors.Passed(ParsedData :: { [string] : unknown })
end

return Compression
