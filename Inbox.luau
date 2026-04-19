--!strict

local DataStoreService = game:GetService("DataStoreService")
local HttpService = game:GetService("HttpService")
local MessagingService = game:GetService("MessagingService")

local Errors = require(script.Parent.Errors)
local Types = require(script.Parent.Types)
local Retry = require(script.Parent.Retry)
local Telemetry = require(script.Parent.Telemetry)
local Compression = require(script.Parent.Compression)

type Result<Value> = Types.Result<Value>

local Inbox = {}
Inbox.__index = Inbox

export type InboxItem = {
	Id : string,
	Sender : string,
	Payload : { [string] : unknown },
	Timestamp : number,
}

export type InboxConfiguration = {
	Name : string,
	Scope : string?,
	CompressionLevel : number?,
	MaxRetries : number?,
}

type InboxEnvelope = {
	_d : string | { InboxItem },
	_c : boolean?,
}

export type InboxManager = typeof(setmetatable({} :: {
	_DataStore : DataStore,
	_Configuration : InboxConfiguration,
	_TopicName : string,
	_DrainCallbacks : { [string] : (Key : string, Items : { InboxItem }) -> () },
	_Subscriptions : { RBXScriptConnection },
}, {} :: { __index : typeof(Inbox) }))

function Inbox.New(Configuration : InboxConfiguration) : InboxManager
	assert(Configuration.Name and #Configuration.Name > 0, "Inbox name is required")

	local self = setmetatable({}, Inbox)
	self._Configuration = Configuration
	self._TopicName = `Nexus_Inbox_{Configuration.Name}`
	self._DrainCallbacks = {}
	self._Subscriptions = {}

	local DataStoreOptions = Instance.new("DataStoreOptions")
	DataStoreOptions.AllScopes = false
	self._DataStore = DataStoreService:GetDataStore(
		`{Configuration.Name}_Inbox`,
		Configuration.Scope or "global",
		DataStoreOptions
	)

	self:_SubscribeToNotifications()

	return self :: any
end

local function DecodeEnvelope(RawData : unknown) : ({ InboxItem }, boolean)
	if RawData == nil then
		return {}, true
	end

	if typeof(RawData) ~= "table" then
		return {}, false
	end

	local Envelope = (RawData :: any) :: InboxEnvelope

	if (Envelope :: any)._items ~= nil then
		local LegacyItems = (Envelope :: any)._items
		if typeof(LegacyItems) == "table" then
			return LegacyItems :: { InboxItem }, true
		end
		return {}, false
	end

	if Envelope._d == nil then
		return {}, true
	end
	if typeof(Envelope._d) == "string" and Envelope._c then
		local DecompressResult = Compression.Decompress(Envelope._d :: string)
		if not DecompressResult.Passed then
			return {}, false
		end
		local DecompressedTable = (DecompressResult :: Types.SuccessResult<{ [string] : unknown }>).Value
		if typeof(DecompressedTable) ~= "table" then
			return {}, false
		end
		return (DecompressedTable :: any) :: { InboxItem }, true
	end

	if typeof(Envelope._d) == "table" then
		return (Envelope._d :: any) :: { InboxItem }, true
	end

	return {}, false
end

local function EncodeEnvelope(Items : { InboxItem }, CompressionLevel : number) : InboxEnvelope
	local JsonString = HttpService:JSONEncode(Items)
	local CompressResult = Compression.CompressString(JsonString, CompressionLevel)

	if CompressResult.Passed then
		return {
			_d = (CompressResult :: Types.SuccessResult<string>).Value,
			_c = true,
		}
	end

	return {
		_d = Items :: any,
		_c = false,
	}
end

function Inbox.Push(
	self : InboxManager,
	RecipientKey : string,
	SenderKey : string,
	Payload : { [string] : unknown }
) : Result<string>
	local ItemId = HttpService:GenerateGUID(false)
	local CompressionLevel = self._Configuration.CompressionLevel or 3
	local StartTime = os.clock()
	local WasCorrupt = false

	local PushResult = Retry.WrapPcall("DataStoreError", function()
		self._DataStore:UpdateAsync(RecipientKey, function(CurrentData : unknown)
			WasCorrupt = false
			local Items, Decoded = DecodeEnvelope(CurrentData)

			if not Decoded then
				WasCorrupt = true
				return nil
			end

			local NewItem : InboxItem = {
				Id = ItemId,
				Sender = SenderKey,
				Payload = Payload,
				Timestamp = os.time(),
			}
			table.insert(Items, NewItem)

			return EncodeEnvelope(Items, CompressionLevel) :: any
		end)
		return true
	end, {
		MaxRetries = self._Configuration.MaxRetries or 3,
		BudgetType = "UpdateAsync",
	} :: any)

	Telemetry.Record("Inbox.Push", os.clock() - StartTime, PushResult.Passed and not WasCorrupt)

	if not PushResult.Passed then
		return Errors.Fail((PushResult :: Types.ErrorResult).Error)
	end

	if WasCorrupt then
		return Errors.Fail(Errors.New("DataStoreError",
			`Inbox for '{RecipientKey}' contains unreadable data. Push aborted to prevent data loss.`,
			{ Key = RecipientKey }
		))
	end

	self:_NotifyRecipient(RecipientKey)

	return Errors.Passed(ItemId)
end

function Inbox.Drain(
	self : InboxManager,
	Key : string
) : Result<{ InboxItem }>
	local DrainedItems : { InboxItem } = {}
	local StartTime = os.clock()

	local DrainResult = Retry.WrapPcall("DataStoreError", function()
		self._DataStore:UpdateAsync(Key, function(CurrentData : unknown)

			DrainedItems = {}

			if CurrentData == nil then
				return nil
			end
			local Items, Decoded = DecodeEnvelope(CurrentData)
			if not Decoded then

				warn(`[Nexus] Inbox.Drain: failed to decode inbox for '{Key}', skipping drain`)
				return nil
			end
			if #Items == 0 then
				return nil
			end
			DrainedItems = Items

			return { _d = {}, _c = false } :: any
		end)
		return true
	end, {
		MaxRetries = self._Configuration.MaxRetries or 3,
		BudgetType = "UpdateAsync",
	} :: any)

	Telemetry.Record("Inbox.Drain", os.clock() - StartTime, DrainResult.Passed)

	if not DrainResult.Passed then
		return Errors.Fail((DrainResult :: Types.ErrorResult).Error)
	end

	return Errors.Passed(DrainedItems)
end

function Inbox.Peek(
	self : InboxManager,
	Key : string
) : Result<{ InboxItem }>
	local StartTime = os.clock()

	local PeekResult = Retry.WrapPcall("DataStoreError", function()
		return self._DataStore:GetAsync(Key)
	end, {
		MaxRetries = self._Configuration.MaxRetries or 3,
		BudgetType = "GetAsync",
	} :: any)

	Telemetry.Record("Inbox.Peek", os.clock() - StartTime, PeekResult.Passed)

	if not PeekResult.Passed then
		return Errors.Fail((PeekResult :: Types.ErrorResult).Error)
	end

	local RawData = (PeekResult :: Types.SuccessResult<unknown>).Value
	local Items, _ = DecodeEnvelope(RawData)
	return Errors.Passed(Items)
end

function Inbox.Count(
	self : InboxManager,
	Key : string
) : Result<number>
	local PeekResult = self:Peek(Key)
	if not PeekResult.Passed then
		return Errors.Fail((PeekResult :: Types.ErrorResult).Error)
	end
	return Errors.Passed(#(PeekResult :: Types.SuccessResult<{ InboxItem }>).Value)
end

function Inbox.OnDrain(
	self : InboxManager,
	Key : string,
	Callback : (Key : string, Items : { InboxItem }) -> ()
) : ()
	self._DrainCallbacks[Key] = Callback
end

function Inbox.OffDrain(self : InboxManager, Key : string) : ()
	self._DrainCallbacks[Key] = nil
end

function Inbox._NotifyRecipient(self : InboxManager, RecipientKey : string) : ()
	task.spawn(function()
		local PayloadString = HttpService:JSONEncode({
			Key = RecipientKey,
		})

		-- Throttled/transient notification failures are common; retry a few
		-- times with exponential backoff. The data write already succeeded,
		-- so a dropped notification only costs reactive drain latency.
		local MaxAttempts = 3
		local Delay = 1
		local LastError : any = nil
		for Attempt = 1, MaxAttempts do
			local PublishSuccess, PublishError = pcall(function()
				MessagingService:PublishAsync(self._TopicName, PayloadString)
				return nil :: any
			end)

			if PublishSuccess then
				return
			end

			LastError = PublishError
			if Attempt < MaxAttempts then
				task.wait(Delay)
				Delay = math.min(Delay * 2, 8)
			end
		end

		warn(`[Nexus] Inbox: notification failed for '{RecipientKey}' after {MaxAttempts} attempts: {LastError}`)
	end)
end

function Inbox._SubscribeToNotifications(self : InboxManager) : ()
	local SubscribeSuccess, Connection = pcall(function()
		return MessagingService:SubscribeAsync(self._TopicName, function(Message : {
			Data : string,
			Sent : number,
		})
			local DecodeSuccess, Payload = pcall(HttpService.JSONDecode, HttpService, Message.Data)
			if not DecodeSuccess then
				return
			end
			local DecodedPayload = Payload :: { [string] : unknown }
			local Key = DecodedPayload.Key
			if typeof(Key) ~= "string" then
				return
			end
			local Callback = self._DrainCallbacks[Key :: string]
			if not Callback then
				return
			end
			local DrainResult = self:Drain(Key :: string)
			if DrainResult.Passed then
				local Items = (DrainResult :: Types.SuccessResult<{ InboxItem }>).Value
				if #Items > 0 then
					Callback(Key :: string, Items)
				end
			end
		end)
	end)

	if SubscribeSuccess then
		(table.insert :: any)(self._Subscriptions, Connection)
	else
		warn(`[Nexus] Inbox: subscription failed for '{self._TopicName}': {Connection}`)
	end
end

function Inbox.Destroy(self : InboxManager) : ()
	for _, Connection in self._Subscriptions do
		Connection:Disconnect()
	end
	table.clear(self._Subscriptions)
	table.clear(self._DrainCallbacks)
end

return Inbox
