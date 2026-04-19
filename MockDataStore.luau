--!strict
-- In-memory DataStore shim for unit tests and offline Studio development.
-- Satisfies the same surface area that Store, Leaderboard, and Inbox rely on
-- (GetAsync, SetAsync, UpdateAsync, RemoveAsync, GetSortedAsync,
-- IncrementAsync, ListKeysAsync, ListVersionsAsync, GetVersionAsync) without
-- touching real Roblox DataStore quota.
--
-- Usage:
--   local MockDataStore = require(Nexus.MockDataStore)
--   local FakeStore = MockDataStore.New("PlayerData")
--   -- Inject it wherever a DataStore is expected, or use MockDataStore.Inject()
--   -- to replace DataStoreService globally for the duration of a test.

local HttpService = game:GetService("HttpService")

local MockDataStore = {}
MockDataStore.__index = MockDataStore

export type MockStoreOptions = {
	SimulateThrottle : boolean?,
	ThrottleRate : number?,
	SimulateFailure : boolean?,
	FailureRate : number?,
	LatencyMs : number?,
}

type VersionEntry = {
	Data : unknown,
	CreatedTime : number,
	IsDeleted : boolean,
	Version : string,
}

type KeyState = {
	Current : unknown,
	Versions : { VersionEntry },
	VersionCounter : number,
}

export type MockStore = {
	_Name : string,
	_Scope : string,
	_Data : { [string] : KeyState },
	_Options : MockStoreOptions,
	_CallCount : number,
	GetAsync : (self : MockStore, Key : string) -> unknown,
	SetAsync : (self : MockStore, Key : string, Value : unknown, UserIds : { number }?, Options : any?) -> (),
	UpdateAsync : (self : MockStore, Key : string, Callback : (Current : unknown) -> (unknown?, { number }?, any?)) -> unknown,
	RemoveAsync : (self : MockStore, Key : string) -> unknown,
	IncrementAsync : (self : MockStore, Key : string, Delta : number?) -> number,
	ListKeysAsync : (self : MockStore, Prefix : string?, PageSize : number?) -> any,
	ListVersionsAsync : (self : MockStore, Key : string, SortDirection : any?, MinDate : number?, MaxDate : number?, PageSize : number?) -> any,
	GetVersionAsync : (self : MockStore, Key : string, VersionId : string) -> (unknown, any),
	GetCallCount : (self : MockStore) -> number,
	Reset : (self : MockStore) -> (),
	Seed : (self : MockStore, Key : string, Value : unknown) -> (),
}

local function ShouldFail(Options : MockStoreOptions) : boolean
	if not Options.SimulateFailure then return false end
	local Rate = Options.FailureRate or 0.1
	return math.random() < Rate
end

local function ApplyLatency(Options : MockStoreOptions) : ()
	if not Options.LatencyMs or Options.LatencyMs <= 0 then return end
	task.wait(Options.LatencyMs / 1000)
end

local function MakeVersionId(Counter : number) : string
	return string.format("V%08d", Counter)
end

local function GetOrCreate(Store : MockStore, Key : string) : KeyState
	local Existing = Store._Data[Key]
	if Existing then return Existing end
	local NewState : KeyState = {
		Current = nil,
		Versions = {},
		VersionCounter = 0,
	}
	Store._Data[Key] = NewState
	return NewState
end

local function RecordVersion(State : KeyState, Data : unknown, IsDeleted : boolean) : string
	State.VersionCounter += 1
	local VersionId = MakeVersionId(State.VersionCounter)
	table.insert(State.Versions, {
		Data = Data,
		CreatedTime = os.time(),
		IsDeleted = IsDeleted,
		Version = VersionId,
	})
	return VersionId
end

function MockDataStore.New(Name : string, Scope : string?, Options : MockStoreOptions?) : MockStore
	local self = setmetatable({}, MockDataStore) :: any
	self._Name = Name
	self._Scope = Scope or "global"
	self._Data = {}
	self._Options = Options or {}
	self._CallCount = 0
	return self
end

function MockDataStore.GetAsync(self : MockStore, Key : string) : unknown
	self._CallCount += 1
	ApplyLatency(self._Options)
	if ShouldFail(self._Options) then
		error("[MockDataStore] Simulated GetAsync failure", 2)
	end
	MockDataStore._CheckFaults(self, "GetAsync")
	local State = self._Data[Key]
	if not State then return nil end
	return State.Current
end

function MockDataStore.SetAsync(
	self : MockStore,
	Key : string,
	Value : unknown,
	_UserIds : { number }?,
	_Options : any?
) : ()
	self._CallCount += 1
	ApplyLatency(self._Options)
	if ShouldFail(self._Options) then
		error("[MockDataStore] Simulated SetAsync failure", 2)
	end

	MockDataStore._CheckFaults(self, "SetAsync")
	local State = GetOrCreate(self, Key)
	State.Current = Value
	RecordVersion(State, Value, Value == nil)
end

function MockDataStore.UpdateAsync(
	self : MockStore,
	Key : string,
	Callback : (Current : unknown) -> (unknown?, { number }?, any?)
) : unknown
	self._CallCount += 1
	ApplyLatency(self._Options)
	if ShouldFail(self._Options) then
		error("[MockDataStore] Simulated UpdateAsync failure", 2)
	end
	MockDataStore._CheckFaults(self, "UpdateAsync")
	local State = GetOrCreate(self, Key)
	local NewValue = Callback(State.Current)
	if NewValue ~= nil then
		State.Current = NewValue
		RecordVersion(State, NewValue, false)
	end
	return State.Current
end

function MockDataStore.RemoveAsync(self : MockStore, Key : string) : unknown
	self._CallCount += 1
	ApplyLatency(self._Options)
	if ShouldFail(self._Options) then
		error("[MockDataStore] Simulated RemoveAsync failure", 2)
	end
	MockDataStore._CheckFaults(self, "RemoveAsync")
	local State = self._Data[Key]
	if not State then return nil end
	local OldValue = State.Current
	State.Current = nil
	RecordVersion(State, nil, true)
	return OldValue
end

function MockDataStore.IncrementAsync(self : MockStore, Key : string, Delta : number?) : number
	self._CallCount += 1
	ApplyLatency(self._Options)
	if ShouldFail(self._Options) then
		error("[MockDataStore] Simulated IncrementAsync failure", 2)
	end

	MockDataStore._CheckFaults(self, "RemoveAsync")
	local State = GetOrCreate(self, Key)
	local Current = State.Current
	local CurrentNum = if typeof(Current) == "number" then Current else 0
	local NewValue = CurrentNum + (Delta or 1)
	State.Current = NewValue
	RecordVersion(State, NewValue, false)
	return NewValue
end

-- Returns a minimal DataStoreKeyPages-shaped table.
function MockDataStore.ListKeysAsync(
	self : MockStore,
	Prefix : string?,
	PageSize : number?
) : any
	self._CallCount += 1
	ApplyLatency(self._Options)
	if ShouldFail(self._Options) then
		error("[MockDataStore] Simulated ListKeysAsync failure", 2)
	end

	MockDataStore._CheckFaults(self, "IncrementAsync")

	local AllKeys : { string } = {}
	for Key in self._Data do
		if self._Data[Key].Current ~= nil then
			if not Prefix or string.sub(Key, 1, #Prefix) == Prefix then
				table.insert(AllKeys, Key)
			end
		end
	end
	table.sort(AllKeys)

	local ResolvedPageSize = math.min(PageSize or 100, 100)
	local Pages : { { KeyName : string } } = {}
	local CurrentPage : { { KeyName : string } } = {}
	for _, Key in AllKeys do
		table.insert(CurrentPage, { KeyName = Key })
		if #CurrentPage >= ResolvedPageSize then
			table.insert(Pages, CurrentPage)
			CurrentPage = {}
		end
	end
	if #CurrentPage > 0 then
		table.insert(Pages, CurrentPage)
	end
	if #Pages == 0 then
		table.insert(Pages, {})
	end

	local PageIndex = 1
	local Cursor = {}
	Cursor.IsFinished = #Pages <= 1
	Cursor.GetCurrentPage = function() return Pages[PageIndex] end
	Cursor.AdvanceToNextPageAsync = function()
		if PageIndex >= #Pages then
			error("[MockDataStore] No more pages", 2)
		end
		PageIndex += 1
		Cursor.IsFinished = PageIndex >= #Pages
	end
	return Cursor
end

function MockDataStore.ListVersionsAsync(
	self : MockStore,
	Key : string,
	_SortDirection : any?,
	_MinDate : number?,
	_MaxDate : number?,
	PageSize : number?
) : any
	self._CallCount += 1
	ApplyLatency(self._Options)
	if ShouldFail(self._Options) then
		error("[MockDataStore] Simulated ListVersionsAsync failure", 2)
	end
	MockDataStore._CheckFaults(self, "ListVersionsAsync")

	local State = self._Data[Key]
	local AllVersions : { VersionEntry } = if State then table.clone(State.Versions) else {}
	table.sort(AllVersions, function(A, B) return A.CreatedTime > B.CreatedTime end)

	local ResolvedPageSize = math.min(PageSize or 10, 100)
	local Pages : { { any } } = {}
	local CurrentPage : { any } = {}
	for _, V in AllVersions do
		table.insert(CurrentPage, { Version = V.Version, CreatedTime = V.CreatedTime, IsDeleted = V.IsDeleted })
		if #CurrentPage >= ResolvedPageSize then
			table.insert(Pages, CurrentPage)
			CurrentPage = {}
		end
	end
	if #CurrentPage > 0 then
		table.insert(Pages, CurrentPage)
	end
	if #Pages == 0 then
		table.insert(Pages, {})
	end

	local PageIndex = 1
	local Cursor = {}
	Cursor.IsFinished = #Pages <= 1
	Cursor.GetCurrentPage = function() return Pages[PageIndex] end
	Cursor.AdvanceToNextPageAsync = function()
		if PageIndex >= #Pages then
			error("[MockDataStore] No more version pages", 2)
		end
		PageIndex += 1
		Cursor.IsFinished = PageIndex >= #Pages
	end
	return Cursor
end

function MockDataStore.GetVersionAsync(
	self : MockStore,
	Key : string,
	VersionId : string
) : (unknown, any)
	self._CallCount += 1
	ApplyLatency(self._Options)
	if ShouldFail(self._Options) then
		error("[MockDataStore] Simulated GetVersionAsync failure", 2)
	end
	MockDataStore._CheckFaults(self, "GetVersionAsync")
	local State = self._Data[Key]
	if not State then return nil, nil end
	for _, V in State.Versions do
		if V.Version == VersionId then
			local KeyInfo = { Version = V.Version, CreatedTime = V.CreatedTime, IsDeleted = V.IsDeleted }
			return V.Data, KeyInfo
		end
	end
	error(`[MockDataStore] Version '{VersionId}' not found for key '{Key}'`, 2)
end

-- GetSortedAsync for OrderedDataStore emulation.
function MockDataStore.GetSortedAsync(
	self : MockStore,
	Ascending : boolean,
	PageSize : number,
	MinValue : number?,
	MaxValue : number?
) : any
	self._CallCount += 1
	ApplyLatency(self._Options)
	if ShouldFail(self._Options) then
		error("[MockDataStore] Simulated GetSortedAsync failure", 2)
	end

	local Entries : { { key : string, value : number } } = {}
	for Key, State in self._Data do
		local Value = State.Current
		if typeof(Value) == "number" then
			if (MinValue == nil or Value >= MinValue) and (MaxValue == nil or Value <= MaxValue) then
				table.insert(Entries, { key = Key, value = Value })
			end
		end
	end

	table.sort(Entries, function(A, B)
		return if Ascending then A.value < B.value else A.value > B.value
	end)

	local ResolvedPageSize = math.min(PageSize, 100)
	local Pages : { { { key : string, value : number } } } = {}
	local CurrentPage : { { key : string, value : number } } = {}
	for _, Entry in Entries do
		table.insert(CurrentPage, Entry)
		if #CurrentPage >= ResolvedPageSize then
			table.insert(Pages, CurrentPage)
			CurrentPage = {}
		end
	end
	if #CurrentPage > 0 then
		table.insert(Pages, CurrentPage)
	end
	if #Pages == 0 then
		table.insert(Pages, {})
	end

	local PageIndex = 1
	local Cursor = {}
	Cursor.IsFinished = #Pages <= 1
	Cursor.GetCurrentPage = function() return Pages[PageIndex] end
	Cursor.AdvanceToNextPageAsync = function()
		if PageIndex >= #Pages then
			error("[MockDataStore] No more sorted pages", 2)
		end
		PageIndex += 1
		Cursor.IsFinished = PageIndex >= #Pages
	end
	return Cursor
end

function MockDataStore.GetCallCount(self : MockStore) : number
	return self._CallCount
end

function MockDataStore.Reset(self : MockStore) : ()
	table.clear(self._Data)
	self._CallCount = 0
end

function MockDataStore.Seed(self : MockStore, Key : string, Value : unknown) : ()
	local State = GetOrCreate(self, Key)
	State.Current = Value
	RecordVersion(State, Value, Value == nil)
end

-- Registry of named mock stores so tests can share state.
local Registry : { [string] : MockStore } = {}

function MockDataStore.GetOrCreate(Name : string, Scope : string?, Options : MockStoreOptions?) : MockStore
	local FullName = Name .. ":" .. (Scope or "global")
	local Existing = Registry[FullName]
	if Existing then return Existing end
	local NewStore = MockDataStore.New(Name, Scope, Options)
	Registry[FullName] = NewStore
	return NewStore
end

function MockDataStore.ClearRegistry() : ()
	table.clear(Registry)
end


local Faults : { [string] : { [string] : { any } } } = {}
local Throttles : { [string] : number } = {}

function MockDataStore.Fault(Name : string, Op : string, ErrorMessage : string) : ()
	Faults[Name] = Faults[Name] or {}
	Faults[Name][Op] = Faults[Name][Op] or {}
	table.insert(Faults[Name][Op], ErrorMessage)
end

function MockDataStore.SimulateThrottle(Name : string, Count : number) : ()
	Throttles[Name] = Count
end

function MockDataStore._CheckFaults(self : MockStore, Op : string) : ()
	if Throttles[self._Name] and Throttles[self._Name] > 0 then
		Throttles[self._Name] -= 1
		error("HTTP 429 (Too Many Requests)", 3)
	end
	if Faults[self._Name] and Faults[self._Name][Op] and #Faults[self._Name][Op] > 0 then
		local Err = table.remove(Faults[self._Name][Op], 1)
		error(Err, 3)
	end
end

local InjectionCount = 0
local OriginalGetDataStore = nil
local OriginalGetOrderedDataStore = nil

function MockDataStore.Inject(Options : MockStoreOptions?) : ()
	InjectionCount += 1
	if InjectionCount > 1 then return end

	local MockProvider = {}
	MockProvider.GetDataStore = function(self, Name : string, Scope : string?)
		return MockDataStore.GetOrCreate(Name, Scope, Options)
	end
	MockProvider.GetOrderedDataStore = function(self, Name : string, Scope : string?)
		return MockDataStore.GetOrCreate(Name, Scope, Options)
	end
	
	_G.NexusMockDataStoreService = MockProvider
end

function MockDataStore.Unject() : ()
	if InjectionCount == 0 then return end
	InjectionCount -= 1
	if InjectionCount == 0 then
		_G.NexusMockDataStoreService = nil
		MockDataStore.ClearRegistry()
		table.clear(Faults)
		table.clear(Throttles)
	end
end

return MockDataStore
