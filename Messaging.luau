--!strict

local MessagingService = game:GetService("MessagingService")
local HttpService = game:GetService("HttpService")

local SERVER_JOB_ID = game.JobId ~= "" and game.JobId or HttpService:GenerateGUID(false)

local Messaging = {}

type InvalidationCallback = (Key : string, NewHolder : string) -> ()

local Subscriptions : { [string] : RBXScriptConnection } = {}
local Callbacks : { [string] : InvalidationCallback } = {}

function Messaging.Subscribe(
	StoreName : string,
	OnInvalidate : InvalidationCallback
) : ()
	if Subscriptions[StoreName] then
		return
	end
	local TopicName = `Nexus_Invalidate_{StoreName}`
	Callbacks[StoreName] = OnInvalidate

	local Success, Connection = pcall(function()
		return MessagingService:SubscribeAsync(TopicName, function(Message : {
			Data : string,
			Sent : number,
		})
			local DecodeSuccess, Payload = pcall(HttpService.JSONDecode, HttpService, Message.Data)
			if not DecodeSuccess then
				return
			end
			local DecodedPayload = Payload :: { [string] : unknown }
			local Key = DecodedPayload.Key
			local NewHolder = DecodedPayload.NewHolder

			if typeof(Key) ~= "string" or typeof(NewHolder) ~= "string" then
				return
			end
			if NewHolder == SERVER_JOB_ID then
				return
			end
			local StoredCallback = Callbacks[StoreName]
			if StoredCallback then
				StoredCallback(Key :: string, NewHolder :: string)
			end
		end)
	end)

	if Success then
		Subscriptions[StoreName] = (Connection :: any) :: RBXScriptConnection
	else
		warn(`[Nexus] Messaging: failed to subscribe to '{TopicName}': {Connection}`)
	end
end

function Messaging.PublishInvalidation(StoreName : string, Key : string) : ()
	local TopicName = `Nexus_Invalidate_{StoreName}`

	task.spawn(function()
		local PayloadString = HttpService:JSONEncode({
			Key = Key,
			NewHolder = SERVER_JOB_ID,
		})

		local PublishSuccess, PublishError = pcall(function() : string?
			MessagingService:PublishAsync(TopicName, PayloadString)
			return nil
		end)

		if not PublishSuccess then
			warn(`[Nexus] Messaging: failed to publish invalidation for '{Key}': {PublishError}`)
		end
	end)
end

function Messaging.Unsubscribe(StoreName : string) : ()
	local Connection = Subscriptions[StoreName]
	if Connection then
		Connection:Disconnect()
		Subscriptions[StoreName] = nil
	end
	Callbacks[StoreName] = nil
end

function Messaging.GetJobId() : string
	return SERVER_JOB_ID
end

return Messaging
