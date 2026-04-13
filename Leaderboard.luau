--!strict

local DataStoreService = game:GetService("DataStoreService")

local Errors = require(script.Parent.Errors)
local Types = require(script.Parent.Types)
local Retry = require(script.Parent.Retry)
local Telemetry = require(script.Parent.Telemetry)

type Result<Value> = Types.Result<Value>

local Leaderboard = {}
Leaderboard.__index = Leaderboard

export type LeaderboardEntry = {
	Key : string,
	Value : number,
}

export type LeaderboardConfiguration = {
	Name : string,
	Scope : string?,
	Ascending : boolean?,
	MaxRetries : number?,
}

export type LeaderboardObject = typeof(setmetatable({} :: {
	_OrderedStore : OrderedDataStore,
	_Configuration : LeaderboardConfiguration,
	_SortDirection : Enum.SortDirection,
}, {} :: { __index : typeof(Leaderboard) }))

function Leaderboard.New(Configuration : LeaderboardConfiguration) : LeaderboardObject
	assert(Configuration.Name and #Configuration.Name > 0, "Leaderboard name is required")

	local self = setmetatable({}, Leaderboard)
	self._Configuration = Configuration
	self._SortDirection = if Configuration.Ascending
		then Enum.SortDirection.Ascending
		else Enum.SortDirection.Descending
	self._OrderedStore = DataStoreService:GetOrderedDataStore(
		Configuration.Name, Configuration.Scope or "global"
	)
	return self :: any
end

function Leaderboard.Set(
	self : LeaderboardObject,
	Key : string,
	Value : number
) : Result<boolean>
	assert(Value == math.floor(Value), "OrderedDataStore only supports integer values")

	local StartTime = os.clock()
	local WriteResult = Retry.WrapPcall("DataStoreError", function()
		self._OrderedStore:SetAsync(Key, Value)
		return true
	end, {
		MaxRetries = self._Configuration.MaxRetries or 3,
		BudgetType = "SetIncrementAsync",
	} :: any)

	Telemetry.Record("Leaderboard.Set", os.clock() - StartTime, WriteResult.Passed)
	return WriteResult
end

function Leaderboard.Increment(
	self : LeaderboardObject,
	Key : string,
	Delta : number?
) : Result<number>
	local IncrementDelta = Delta or 1
	assert(IncrementDelta == math.floor(IncrementDelta), "Increment delta must be an integer")

	local StartTime = os.clock()
	local IncrementResult = Retry.WrapPcall("DataStoreError", function()
		return self._OrderedStore:IncrementAsync(Key, IncrementDelta)
	end, {
		MaxRetries = self._Configuration.MaxRetries or 3,
		BudgetType = "SetIncrementAsync",
	} :: any)

	Telemetry.Record("Leaderboard.Increment", os.clock() - StartTime, IncrementResult.Passed)
	if not IncrementResult.Passed then
		return Errors.Fail((IncrementResult :: Types.ErrorResult).Error)
	end
	return Errors.Passed((IncrementResult :: Types.SuccessResult<unknown>).Value :: number)
end

function Leaderboard.Remove(
	self : LeaderboardObject,
	Key : string
) : Result<boolean>
	local StartTime = os.clock()
	local RemoveResult = Retry.WrapPcall("DataStoreError", function()
		self._OrderedStore:RemoveAsync(Key)
		return true
	end, {
		MaxRetries = self._Configuration.MaxRetries or 3,
		BudgetType = "SetIncrementAsync",
	} :: any)

	Telemetry.Record("Leaderboard.Remove", os.clock() - StartTime, RemoveResult.Passed)
	return RemoveResult
end

function Leaderboard.Get(
	self : LeaderboardObject,
	Key : string
) : Result<number?>
	local StartTime = os.clock()
	local GetResult = Retry.WrapPcall("DataStoreError", function()
		return self._OrderedStore:GetAsync(Key)
	end, {
		MaxRetries = self._Configuration.MaxRetries or 3,
		BudgetType = "GetAsync",
	} :: any)

	Telemetry.Record("Leaderboard.Get", os.clock() - StartTime, GetResult.Passed)
	if not GetResult.Passed then
		return Errors.Fail((GetResult :: Types.ErrorResult).Error)
	end
	return Errors.Passed((GetResult :: Types.SuccessResult<unknown>).Value :: number?)
end

function Leaderboard.GetTop(
	self : LeaderboardObject,
	Count : number,
	MinValue : number?,
	MaxValue : number?
) : Result<{ LeaderboardEntry }>
	assert(Count >= 1, "Count must be at least 1")

	local PageSize = math.min(Count, 100)
	local StartTime = os.clock()
	local PagesResult = Retry.WrapPcall("DataStoreError", function()
		return self._OrderedStore:GetSortedAsync(
			self._SortDirection == Enum.SortDirection.Ascending, PageSize, MinValue, MaxValue
		)
	end, {
		MaxRetries = self._Configuration.MaxRetries or 3,
		BudgetType = "GetSortedAsync",
	} :: any)

	if not PagesResult.Passed then
		Telemetry.Record("Leaderboard.GetTop", os.clock() - StartTime, false)
		return Errors.Fail((PagesResult :: Types.ErrorResult).Error)
	end

	local Pages = (PagesResult :: Types.SuccessResult<DataStorePages>).Value
	local Entries : { LeaderboardEntry } = {}

	local CurrentPage = Pages:GetCurrentPage()
	for _, Item in CurrentPage do
		table.insert(Entries, {
			Key = Item.key :: string,
			Value = Item.value :: number,
		} :: LeaderboardEntry)
	end

	while #Entries < Count and not Pages.IsFinished do
		local AdvanceSuccess, AdvanceError = pcall(function()
			Pages:AdvanceToNextPageAsync()
			return nil :: any
		end)
		if not AdvanceSuccess then
			warn(`[Nexus] Leaderboard.GetTop: page advance failed: {AdvanceError}`)
			break
		end

		local NextPage = Pages:GetCurrentPage()
		for _, Item in NextPage do
			table.insert(Entries, {
				Key = Item.key :: string,
				Value = Item.value :: number,
			} :: LeaderboardEntry)
			if #Entries >= Count then
				break
			end
		end
	end

	Telemetry.Record("Leaderboard.GetTop", os.clock() - StartTime, true)
	return Errors.Passed(Entries)
end

function Leaderboard.GetRange(
	self : LeaderboardObject,
	PageSize : number,
	MinValue : number?,
	MaxValue : number?
) : Result<{ LeaderboardEntry }>
	assert(PageSize >= 1 and PageSize <= 100, "PageSize must be between 1 and 100")

	local StartTime = os.clock()
	local PagesResult = Retry.WrapPcall("DataStoreError", function()
		return self._OrderedStore:GetSortedAsync(
			self._SortDirection == Enum.SortDirection.Ascending, PageSize, MinValue, MaxValue
		)
	end, {
		MaxRetries = self._Configuration.MaxRetries or 3,
		BudgetType = "GetSortedAsync",
	} :: any)

	if not PagesResult.Passed then
		Telemetry.Record("Leaderboard.GetRange", os.clock() - StartTime, false)
		return Errors.Fail((PagesResult :: Types.ErrorResult).Error)
	end

	local Pages = (PagesResult :: Types.SuccessResult<DataStorePages>).Value
	local Entries : { LeaderboardEntry } = {}

	local CurrentPage = Pages:GetCurrentPage()
	for _, Item in CurrentPage do
		table.insert(Entries, {
			Key = Item.key :: string,
			Value = Item.value :: number,
		} :: LeaderboardEntry)
	end

	Telemetry.Record("Leaderboard.GetRange", os.clock() - StartTime, true)
	return Errors.Passed(Entries)
end

function Leaderboard.GetRank(
	self : LeaderboardObject,
	Key : string,
	MaxScan : number?
) : Result<{ Rank : number, Value : number }?>
	local ScanLimit = MaxScan or 1000

	local StartTime = os.clock()
	local PagesResult = Retry.WrapPcall("DataStoreError", function()
		return self._OrderedStore:GetSortedAsync(self._SortDirection == Enum.SortDirection.Ascending, 100)
	end, {
		MaxRetries = self._Configuration.MaxRetries or 3,
		BudgetType = "GetSortedAsync",
	} :: any)

	if not PagesResult.Passed then
		Telemetry.Record("Leaderboard.GetRank", os.clock() - StartTime, false)
		return Errors.Fail((PagesResult :: Types.ErrorResult).Error)
	end

	local Pages = (PagesResult :: Types.SuccessResult<DataStorePages>).Value
	local CurrentRank = 0

	while true do
		local CurrentPage = Pages:GetCurrentPage()
		for _, Item in CurrentPage do
			CurrentRank += 1
			if Item.key == Key then
				Telemetry.Record("Leaderboard.GetRank", os.clock() - StartTime, true)
				return Errors.Passed({ Rank = CurrentRank, Value = Item.value :: number } :: { Rank : number, Value : number }?)
			end
		end

		if Pages.IsFinished or CurrentRank >= ScanLimit then
			break
		end

		local AdvanceSuccess = pcall(function()
			Pages:AdvanceToNextPageAsync()
			return nil :: any
		end)
		if not AdvanceSuccess then
			break
		end
	end

	Telemetry.Record("Leaderboard.GetRank", os.clock() - StartTime, true)
	return Errors.Passed(nil :: { Rank : number, Value : number }?)
end

return Leaderboard
