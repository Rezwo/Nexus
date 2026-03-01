--!strict

local HttpService = game:GetService("HttpService")
local EncodingService = game:GetService("EncodingService")

local Errors = require(script.Parent.Errors)
local Types = require(script.Parent.Types)

type Result<Value> = Types.Result<Value>

local Compression = {}

local DEFAULT_COMPRESSION_LEVEL = 3

function Compression.Compress(
	Data : { [string] : unknown },
	CompressionLevel : number?
) : Result<string>
	local EncodeSuccess, JsonString = pcall(HttpService.JSONEncode, HttpService, Data)
	if not EncodeSuccess then
		return Errors.Fail(Errors.New("CompressionError", `Failed to JSON-encode: {JsonString}`))
	end

	local Level = CompressionLevel or DEFAULT_COMPRESSION_LEVEL
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
	local Level = CompressionLevel or DEFAULT_COMPRESSION_LEVEL
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
	local ZstdResult = Compression._DecompressZstd(EncodedData)
	if ZstdResult.Passed then
		return ZstdResult
	end

	local LegacyResult = Compression._DecompressLegacyRle(EncodedData)
	if LegacyResult.Passed then
		return LegacyResult
	end

	return ZstdResult
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

local BASE64_CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
local LegacyBase64DecodeTable : { [number] : number } = {}
for Index = 0, 63 do
	LegacyBase64DecodeTable[string.byte(BASE64_CHARACTERS, Index + 1)] = Index
end

function Compression._LegacyBase64Decode(EncodedData : string) : string
	local DataLength = #EncodedData
	while DataLength > 0 and (string.byte(EncodedData, DataLength) == 61 or string.byte(EncodedData, DataLength) == 10) do
		DataLength -= 1
	end

	local OutputLength = math.floor(DataLength * 3 / 4)
	local OutputBuffer = buffer.create(OutputLength)
	local WritePosition = 0
	local ReadPosition = 1

	while ReadPosition <= DataLength - 3 do
		local CharA = LegacyBase64DecodeTable[string.byte(EncodedData, ReadPosition)] or 0
		local CharB = LegacyBase64DecodeTable[string.byte(EncodedData, ReadPosition + 1)] or 0
		local CharC = LegacyBase64DecodeTable[string.byte(EncodedData, ReadPosition + 2)] or 0
		local CharD = LegacyBase64DecodeTable[string.byte(EncodedData, ReadPosition + 3)] or 0

		buffer.writeu8(OutputBuffer, WritePosition, bit32.bor(bit32.lshift(CharA, 2), bit32.rshift(CharB, 4)))
		buffer.writeu8(OutputBuffer, WritePosition + 1, bit32.bor(bit32.lshift(bit32.band(CharB, 15), 4), bit32.rshift(CharC, 2)))
		buffer.writeu8(OutputBuffer, WritePosition + 2, bit32.bor(bit32.lshift(bit32.band(CharC, 3), 6), CharD))

		WritePosition += 3
		ReadPosition += 4
	end

	local RemainingChars = DataLength - ReadPosition + 1
	if RemainingChars >= 2 then
		local CharA = LegacyBase64DecodeTable[string.byte(EncodedData, ReadPosition)] or 0
		local CharB = LegacyBase64DecodeTable[string.byte(EncodedData, ReadPosition + 1)] or 0
		buffer.writeu8(OutputBuffer, WritePosition, bit32.bor(bit32.lshift(CharA, 2), bit32.rshift(CharB, 4)))
		WritePosition += 1
		if RemainingChars >= 3 then
			local CharC = LegacyBase64DecodeTable[string.byte(EncodedData, ReadPosition + 2)] or 0
			buffer.writeu8(OutputBuffer, WritePosition, bit32.bor(bit32.lshift(bit32.band(CharB, 15), 4), bit32.rshift(CharC, 2)))
			WritePosition += 1
		end
	end

	return buffer.readstring(OutputBuffer, 0, WritePosition)
end

function Compression._DecompressLegacyRle(EncodedData : string) : Result<{ [string] : unknown }>
	local DecodeSuccess, DecodedBytes = pcall(Compression._LegacyBase64Decode, EncodedData)
	if not DecodeSuccess then
		return Errors.Fail(Errors.New("CompressionError", `Legacy base64 decode failed: {DecodedBytes}`))
	end

	local RawString = DecodedBytes :: string
	local RawLength = #RawString
	local DecompressedParts : { string } = {}
	local ReadPosition = 1

	while ReadPosition <= RawLength do
		local CurrentByte = string.byte(RawString, ReadPosition)
		if CurrentByte == 0 and ReadPosition + 2 <= RawLength then
			local RepeatCount = string.byte(RawString, ReadPosition + 1)
			local RepeatedByte = string.byte(RawString, ReadPosition + 2)
			table.insert(DecompressedParts, string.rep(string.char(RepeatedByte), RepeatCount))
			ReadPosition += 3
		else
			table.insert(DecompressedParts, string.char(CurrentByte))
			ReadPosition += 1
		end
	end

	local JsonString = table.concat(DecompressedParts)
	local ParseSuccess, ParsedData = pcall(HttpService.JSONDecode, HttpService, JsonString)
	if not ParseSuccess then
		return Errors.Fail(Errors.New("CompressionError", `Legacy JSON decode failed: {ParsedData}`))
	end
	return Errors.Passed(ParsedData :: { [string] : unknown })
end

return Compression
