--!strict

local Players = game:GetService("Players")

local Types = require(script.Parent.Types)
local Errors = require(script.Parent.Errors)
local Signal = require(script.Parent.Signal)
local Store = require(script.Parent.Store)
local Telemetry = require(script.Parent.Telemetry)
local InboxModule = require(script.Parent.Inbox)
local Validation = require(script.Parent.Validation)

type Result<Value> = Types.Result<Value>
type NexusError = Types.NexusError
type StoreConfiguration<DataType> = Types.StoreConfiguration<DataType>
type Signal<T...> = Types.Signal<T...>
type Connection = Types.Connection
type InboxItem = InboxModule.InboxItem

local PlayerStore = {}
PlayerStore.__index = PlayerStore

export type PlayerStoreConfiguration<DataType> = {
	Name : string,
	Schema : Types.SchemaDefinition<DataType>,
	Scope : string?,
	AutoSaveInterval : number?,
	Compress : boolean?,
	CompressionThreshold : number?,
	CompressionLevel : number?,
	MaxRetries : number?,
	Strict : boolean?,
	ShardThreshold : number?,
	LockRetryTimeout : number?,
	KickOnLoadFailure : boolean?,
	KickMessage : string?,
	Inbox : boolean?,
	ProcessGift : ((Data : DataType, Gift : InboxItem) -> DataType)?,
}

export type PlayerStoreObject<DataType> = {

	_Store : Store.StoreObject<DataType>,
	_Configuration : PlayerStoreConfiguration<DataType>,
	_Connections : { RBXScriptConnection },
	_LoadingPlayers : { [number] : boolean },
	_Inbox : InboxModule.InboxManager?,

	OnPlayerLoaded : Signal<Player, DataType>,
	OnPlayerSaved : Signal<Player, DataType>,
	OnPlayerReleased : Signal<Player>,
	OnPlayerError : Signal<Player, NexusError>,
	OnGiftReceived : Signal<Player, { InboxItem }>,

	ReadData : (self : PlayerStoreObject<DataType>, TargetPlayer : Player) -> DataType?,
	UpdateData : (self : PlayerStoreObject<DataType>, TargetPlayer : Player, Transform : (Data : DataType) -> DataType?) -> Result<DataType>,
	SaveData : (self : PlayerStoreObject<DataType>, TargetPlayer : Player) -> Result<boolean>,
	IsLoaded : (self : PlayerStoreObject<DataType>, TargetPlayer : Player) -> boolean,
	IsLoading : (self : PlayerStoreObject<DataType>, TargetPlayer : Player) -> boolean,
	GetInfo : (self : PlayerStoreObject<DataType>, TargetPlayer : Player) -> Types.SessionInfo?,
	GetStore : (self : PlayerStoreObject<DataType>) -> Store.StoreObject<DataType>,
	GetInbox : (self : PlayerStoreObject<DataType>) -> InboxModule.InboxManager?,
	SendGift : (self : PlayerStoreObject<DataType>, FromPlayer : Player, ToKey : string | number, GiftPayload : { [string] : unknown }, DeductTransform : ((Data : DataType) -> DataType?)?) -> Result<string>,
	DrainInbox : (self : PlayerStoreObject<DataType>, TargetPlayer : Player) -> Result<{ InboxItem }>,
	PeekInbox : (self : PlayerStoreObject<DataType>, Key : string | number) -> Result<{ InboxItem }>,
	Destroy : (self : PlayerStoreObject<DataType>) -> (),

	_LoadPlayer : (self : PlayerStoreObject<DataType>, TargetPlayer : Player) -> (),
	_UnloadPlayer : (self : PlayerStoreObject<DataType>, TargetPlayer : Player) -> (),
}

function PlayerStore.New<DataType>(
	Configuration : PlayerStoreConfiguration<DataType>
) : PlayerStoreObject<DataType>
	assert(Configuration.Name and #Configuration.Name > 0, "Store name is required")
	assert(Configuration.Schema, "Schema is required")

	local self = setmetatable({}, PlayerStore) :: any

	local StoreConfig : StoreConfiguration<DataType> = {
		Name = Configuration.Name,
		Scope = Configuration.Scope,
		Schema = Configuration.Schema,
		LockMode = "Session",
		AutoSaveInterval = Configuration.AutoSaveInterval or 300,
		Compress = Configuration.Compress,
		CompressionThreshold = Configuration.CompressionThreshold,
		CompressionLevel = Configuration.CompressionLevel,
		MaxRetries = Configuration.MaxRetries,
		Strict = Configuration.Strict,
		ShardThreshold = Configuration.ShardThreshold,
		GetUserIds = function(Key : string) : { number }
			local UserId = tonumber(Key)
			if UserId then
				return { UserId }
			end
			return {}
		end,
	}

	self._Store = Store.New(StoreConfig)
	self._Configuration = Configuration
	self._Connections = {}
	self._LoadingPlayers = {}
	self._Inbox = nil

	if Configuration.Inbox then
		self._Inbox = InboxModule.New({
			Name = Configuration.Name,
			Scope = Configuration.Scope,
			MaxRetries = Configuration.MaxRetries,
		})
	end

	self.OnPlayerLoaded = Signal.New()
	self.OnPlayerSaved = Signal.New()
	self.OnPlayerReleased = Signal.New()
	self.OnPlayerError = Signal.New()
	self.OnGiftReceived = Signal.New()

	self._Store.OnSave:Connect(function(Key : string, Data : DataType)
		local TargetPlayer = Players:GetPlayerByUserId(tonumber(Key) or 0)
		if TargetPlayer then
			self.OnPlayerSaved:Fire(TargetPlayer, Data)
		end
	end)

	self._Store.OnRelease:Connect(function(Key : string)
		local TargetPlayer = Players:GetPlayerByUserId(tonumber(Key) or 0)
		if TargetPlayer then
			self.OnPlayerReleased:Fire(TargetPlayer)
		end
	end)

	self._Store.OnError:Connect(function(Key : string, ErrorValue : NexusError)
		local TargetPlayer = Players:GetPlayerByUserId(tonumber(Key) or 0)
		if TargetPlayer then
			self.OnPlayerError:Fire(TargetPlayer, ErrorValue)
		end
	end)

	for _, ExistingPlayer in Players:GetPlayers() do
		task.spawn(function()
			self:_LoadPlayer(ExistingPlayer)
		end)
	end

	local AddedConnection = Players.PlayerAdded:Connect(function(JoinedPlayer)
		self:_LoadPlayer(JoinedPlayer)
	end)
	table.insert(self._Connections, AddedConnection)

	local RemovingConnection = Players.PlayerRemoving:Connect(function(LeavingPlayer)
		self:_UnloadPlayer(LeavingPlayer)
	end)
	table.insert(self._Connections, RemovingConnection)

	return self
end

local function MergeGifts<DataType>(
	Data : DataType,
	Items : { InboxModule.InboxItem },
	ProcessGift : (Data : DataType, Gift : InboxModule.InboxItem) -> DataType
) : DataType
	local CurrentData = Data
	for _, Gift in Items do
		local MergedData = ProcessGift(CurrentData, Gift)
		if MergedData ~= nil then
			CurrentData = MergedData
		else
			warn(`[Nexus] ProcessGift returned nil for gift '{Gift.Id}' from '{Gift.Sender}', skipping`)
		end
	end
	return CurrentData
end

function PlayerStore.ReadData<DataType>(
	self : PlayerStoreObject<DataType>,
	TargetPlayer : Player
) : DataType?
	local Key = tostring(TargetPlayer.UserId)
	local ReadResult = self._Store:Read(Key)
	if ReadResult.Passed then
		return (ReadResult :: Types.SuccessResult<DataType>).Value
	end
	return nil
end

function PlayerStore.UpdateData<DataType>(
	self : PlayerStoreObject<DataType>,
	TargetPlayer : Player,
	Transform : (Data : DataType) -> DataType?
) : Result<DataType>
	local Key = tostring(TargetPlayer.UserId)
	return self._Store:Update(Key, Transform)
end

function PlayerStore.SaveData<DataType>(
	self : PlayerStoreObject<DataType>,
	TargetPlayer : Player
) : Result<boolean>
	local Key = tostring(TargetPlayer.UserId)
	return self._Store:Save(Key)
end

function PlayerStore.IsLoaded<DataType>(
	self : PlayerStoreObject<DataType>,
	TargetPlayer : Player
) : boolean
	return self._Store:IsLoaded(tostring(TargetPlayer.UserId))
end

function PlayerStore.IsLoading<DataType>(
	self : PlayerStoreObject<DataType>,
	TargetPlayer : Player
) : boolean
	return self._LoadingPlayers[TargetPlayer.UserId] == true
end

function PlayerStore.GetInfo<DataType>(
	self : PlayerStoreObject<DataType>,
	TargetPlayer : Player
) : Types.SessionInfo?
	return self._Store:GetSessionInfo(tostring(TargetPlayer.UserId))
end

function PlayerStore.GetStore<DataType>(
	self : PlayerStoreObject<DataType>
) : Store.StoreObject<DataType>
	return self._Store
end

function PlayerStore.GetInbox<DataType>(
	self : PlayerStoreObject<DataType>
) : InboxModule.InboxManager?
	return self._Inbox
end

function PlayerStore.SendGift<DataType>(
	self : PlayerStoreObject<DataType>,
	FromPlayer : Player,
	ToKey : string | number,
	GiftPayload : { [string] : unknown },
	DeductTransform : ((Data : DataType) -> DataType?)?
) : Result<string>
	if not self._Inbox then
		return Errors.Fail(Errors.New("Internal",
			"Inbox is not enabled. Set Inbox = true in PlayerStore configuration.",
			{}
		))
	end

	local FromKey = tostring(FromPlayer.UserId)
	local RecipientKey = tostring(ToKey)

	local PreDeductSnapshot : DataType? = nil
	local DeductionCancelled = false
	if DeductTransform then
		local DeductResult = self._Store:Update(FromKey, function(Data : DataType) : DataType?
			PreDeductSnapshot = Validation.DeepClone(Data) :: DataType
			local Result = DeductTransform(Data)
			if Result == nil then
				DeductionCancelled = true
			end
			return Result
		end)
		if not DeductResult.Passed then
			return Errors.Fail((DeductResult :: Types.ErrorResult).Error)
		end

		if DeductionCancelled then
			return Errors.Fail(Errors.New("Internal",
				"Gift cancelled: DeductTransform returned nil.",
				{ Key = FromKey }
			))
		end
	end

	local StartTime = os.clock()
	local PushResult = self._Inbox:Push(RecipientKey, FromKey, GiftPayload)

	if not PushResult.Passed then

		if PreDeductSnapshot ~= nil then
			local RollbackResult = self._Store:Update(FromKey, function(_ : DataType) : DataType?
				return PreDeductSnapshot
			end)
			if not RollbackResult.Passed then
				warn(`[Nexus] PlayerStore.SendGift: rollback failed for sender '{FromKey}', data may be inconsistent`)
			end
		end
		Telemetry.Record("PlayerStore.SendGift", os.clock() - StartTime, false)
		return Errors.Fail((PushResult :: Types.ErrorResult).Error)
	end

	Telemetry.Record("PlayerStore.SendGift", os.clock() - StartTime, true)
	return Errors.Passed((PushResult :: Types.SuccessResult<string>).Value)
end

function PlayerStore.DrainInbox<DataType>(
	self : PlayerStoreObject<DataType>,
	TargetPlayer : Player
) : Result<{ InboxItem }>
	if not self._Inbox then
		return Errors.Passed({})
	end

	local Key = tostring(TargetPlayer.UserId)
	local DrainResult = self._Inbox:Drain(Key)

	if not DrainResult.Passed then
		return Errors.Fail((DrainResult :: Types.ErrorResult).Error)
	end

	local Items = (DrainResult :: Types.SuccessResult<{ InboxItem }>).Value
	if #Items == 0 then
		return Errors.Passed(Items)
	end

	local ProcessGift = self._Configuration.ProcessGift
	if ProcessGift then
		local UpdateResult = self._Store:Update(Key, function(Data : DataType) : DataType?
			return MergeGifts(Data, Items, ProcessGift)
		end)

		if not UpdateResult.Passed then
			warn(`[Nexus] PlayerStore: gift processing failed for '{Key}', gifts may be lost`)
		end
	end

	self.OnGiftReceived:Fire(TargetPlayer, Items)

	return Errors.Passed(Items)
end

function PlayerStore.PeekInbox<DataType>(
	self : PlayerStoreObject<DataType>,
	Key : string | number
) : Result<{ InboxItem }>
	if not self._Inbox then
		return Errors.Passed({})
	end
	return self._Inbox:Peek(tostring(Key))
end

function PlayerStore.Destroy<DataType>(self : PlayerStoreObject<DataType>) : ()
	for _, Connection in self._Connections do
		Connection:Disconnect()
	end
	table.clear(self._Connections)

	if self._Inbox then
		self._Inbox:Destroy()
	end

	self._Store:Destroy()

	self.OnPlayerLoaded:Destroy()
	self.OnPlayerSaved:Destroy()
	self.OnPlayerReleased:Destroy()
	self.OnPlayerError:Destroy()
	self.OnGiftReceived:Destroy()
end

function PlayerStore._LoadPlayer<DataType>(
	self : PlayerStoreObject<DataType>,
	TargetPlayer : Player
) : ()
	local UserId = TargetPlayer.UserId
	local Key = tostring(UserId)


	if self._Store:IsLoaded(Key) or self._LoadingPlayers[UserId] then
		return
	end

	self._LoadingPlayers[UserId] = true

	local StartTime = os.clock()
	local LoadResult : Result<DataType>

	local RetryTimeout = self._Configuration.LockRetryTimeout or 0
	if RetryTimeout > 0 then

		local DeadlineTime = os.clock() + RetryTimeout
		while true do
			LoadResult = self._Store:Load(Key)
			if LoadResult.Passed then
				break
			end
			local LoadError = (LoadResult :: Types.ErrorResult).Error
			if LoadError.Kind ~= "SessionLocked" then
				break
			end
			if os.clock() >= DeadlineTime then
				break
			end
			task.wait(2)


			if not TargetPlayer:IsDescendantOf(Players) then
				self._LoadingPlayers[UserId] = nil
				return
			end
		end
	else
		LoadResult = self._Store:Load(Key)
	end

	self._LoadingPlayers[UserId] = nil
	Telemetry.Record("PlayerStore.Load", os.clock() - StartTime, LoadResult.Passed)


	if not TargetPlayer:IsDescendantOf(Players) then
		if LoadResult.Passed then
			self._Store:Close(Key)
		end
		return
	end

	if LoadResult.Passed then
		local Data = (LoadResult :: Types.SuccessResult<DataType>).Value
		self.OnPlayerLoaded:Fire(TargetPlayer, Data)

		if self._Inbox then
			task.spawn(function()

				self._Inbox:OnDrain(Key, function(_DrainKey : string, Items : { InboxItem })

					if not TargetPlayer:IsDescendantOf(Players) then
						return
					end
					local ProcessGift = self._Configuration.ProcessGift
					if ProcessGift and self._Store:IsLoaded(Key) then
						self._Store:Update(Key, function(CurrentData : DataType) : DataType?
							return MergeGifts(CurrentData, Items, ProcessGift)
						end)
					end

					self.OnGiftReceived:Fire(TargetPlayer, Items)
				end)

				local DrainResult = self._Inbox:Drain(Key)
				if DrainResult.Passed then
					local Items = (DrainResult :: Types.SuccessResult<{ InboxItem }>).Value
					if #Items > 0 then
						local ProcessGift = self._Configuration.ProcessGift
						if ProcessGift and self._Store:IsLoaded(Key) then
							self._Store:Update(Key, function(CurrentData : DataType) : DataType?
								return MergeGifts(CurrentData, Items, ProcessGift)
							end)
						end

						self.OnGiftReceived:Fire(TargetPlayer, Items)
					end
				end
			end)
		end
	else
		local LoadError = (LoadResult :: Types.ErrorResult).Error
		warn(`[Nexus] PlayerStore: failed to load {TargetPlayer.Name} ({UserId}): {LoadError.Message}`)
		self.OnPlayerError:Fire(TargetPlayer, LoadError)

		local ShouldKick = if self._Configuration.KickOnLoadFailure ~= nil
			then self._Configuration.KickOnLoadFailure
			else true

		if ShouldKick then
			local KickMessage = self._Configuration.KickMessage
				or "Failed to load your data. Please rejoin."
			TargetPlayer:Kick(KickMessage)
		end
	end
end

function PlayerStore._UnloadPlayer<DataType>(
	self : PlayerStoreObject<DataType>,
	TargetPlayer : Player
) : ()
	local Key = tostring(TargetPlayer.UserId)
	self._LoadingPlayers[TargetPlayer.UserId] = nil

	if self._Inbox then
		self._Inbox:OffDrain(Key)
	end

	if self._Store:IsLoaded(Key) then
		local StartTime = os.clock()
		self._Store:Close(Key)
		Telemetry.Record("PlayerStore.Close", os.clock() - StartTime, true)
	end
end

return PlayerStore
