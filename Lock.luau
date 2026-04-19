--!strict

local MemoryStoreService = game:GetService("MemoryStoreService")
local HttpService = game:GetService("HttpService")

local Errors = require(script.Parent.Errors)
local Types = require(script.Parent.Types)
local Circuit = require(script.Parent.Circuit)
local Messaging = require(script.Parent.Messaging)

type Result<Value> = Types.Result<Value>
type LockInfo = Types.LockInfo
type NexusError = Types.NexusError

local Lock = {}
Lock.__index = Lock

local DEFAULT_LEASE_TIME_TO_LIVE = 60
local DEFAULT_HEARTBEAT_INTERVAL = 15
local DEFAULT_ENVELOPE_LEASE_TIME_TO_LIVE = 120
-- Allow some clock drift across servers. Leases within this tolerance of expiry
-- are still considered held — prevents premature steals due to time skew.
local DEFAULT_CLOCK_DRIFT_TOLERANCE = 5
-- If a heartbeat renewal takes longer than this (seconds), treat it as lost.
local DEFAULT_HEARTBEAT_WATCHDOG = 30

local SERVER_JOB_ID = game.JobId ~= "" and game.JobId or HttpService:GenerateGUID(false)

export type Lease = {
	JobId : string,
	ExpiresAt : number,
	LeaseVersion : number,
	RenewedAt : number,
}

export type LockConfiguration = {
	LeaseTimeToLive : number?,
	HeartbeatInterval : number?,
	EnvelopeLeaseTimeToLive : number?,
	ClockDriftTolerance : number?,
	HeartbeatWatchdog : number?,
	CircuitFailureThreshold : number?,
	CircuitRecoveryTimeout : number?,
	CircuitHalfOpenSuccessThreshold : number?,
}

type AcquireOutcome =
	{ Tag : "Acquired", Lease : Lease, StolenFrom : string? }
	| { Tag : "LockedByOther", Holder : string }
	| { Tag : "ServiceError", Message : string }

export type LockManager = typeof(setmetatable({} :: {
	_HashMap : MemoryStoreHashMap,
	_StoreName : string,
	_ActiveHeartbeats : { [string] : thread },
	_ActiveLeases : { [string] : Lease },
	_HeartbeatGeneration : { [string] : number },
	_Circuit : Circuit.CircuitBreaker,
	_LeaseTimeToLive : number,
	_HeartbeatInterval : number,
	_EnvelopeLeaseTimeToLive : number,
	_ClockDriftTolerance : number,
	_HeartbeatWatchdog : number,
	_OnStolen : ((Key : string, NewHolder : string) -> ())?,
}, {} :: { __index : typeof(Lock) }))

local function IsValidEnvelopeLock(EnvelopeLock : unknown) : boolean
	if typeof(EnvelopeLock) ~= "table" then return false end
	local Table = EnvelopeLock :: any
	return typeof(Table.jobId) == "string"
		and typeof(Table.renewedAt) == "number"
		and typeof(Table.lockVersion) == "number"
end

function Lock.New(StoreName : string, Configuration : LockConfiguration?) : LockManager
	local Config : LockConfiguration = (Configuration or {}) :: any
	local self = setmetatable({}, Lock)
	self._HashMap = MemoryStoreService:GetHashMap(`Nexus_Lease_{StoreName}`)
	self._StoreName = StoreName
	self._ActiveHeartbeats = {}
	self._ActiveLeases = {}
	self._HeartbeatGeneration = {}
	self._LeaseTimeToLive = Config.LeaseTimeToLive or DEFAULT_LEASE_TIME_TO_LIVE
	self._HeartbeatInterval = Config.HeartbeatInterval or DEFAULT_HEARTBEAT_INTERVAL
	self._EnvelopeLeaseTimeToLive = Config.EnvelopeLeaseTimeToLive or DEFAULT_ENVELOPE_LEASE_TIME_TO_LIVE
	self._ClockDriftTolerance = Config.ClockDriftTolerance or DEFAULT_CLOCK_DRIFT_TOLERANCE
	self._HeartbeatWatchdog = Config.HeartbeatWatchdog or DEFAULT_HEARTBEAT_WATCHDOG
	self._Circuit = Circuit.New({
		Name = `MemoryStore:{StoreName}`,
		FailureThreshold = Config.CircuitFailureThreshold or 3,
		RecoveryTimeout = Config.CircuitRecoveryTimeout or 30,
		HalfOpenSuccessThreshold = Config.CircuitHalfOpenSuccessThreshold or 2,
	})
	self._OnStolen = nil
	return self :: any
end

function Lock.SetStolenHandler(self : LockManager, Handler : ((Key : string, NewHolder : string) -> ())?) : ()
	self._OnStolen = Handler
end

function Lock.Acquire(
	self : LockManager,
	Key : string,
	EnvelopeLock : LockInfo?
) : Result<Lease>
	local CurrentTime = os.time()
	local EnvelopeOverride = false

	-- Defensive: never trust structurally-malformed envelope locks.
	local SafeEnvelopeLock : LockInfo? = if EnvelopeLock ~= nil and IsValidEnvelopeLock(EnvelopeLock)
		then EnvelopeLock else nil
	if EnvelopeLock ~= nil and SafeEnvelopeLock == nil then
		warn(`[Nexus] Lock.Acquire: ignoring malformed envelope lock for '{Key}'`)
	end

	if self._Circuit:ShouldAttemptCall() then
		local Outcome = self:_TryMemoryStore(Key, CurrentTime)

		if Outcome.Tag == "Acquired" then
			self._ActiveLeases[Key] = Outcome.Lease
			self:_StartHeartbeat(Key)

			if Outcome.StolenFrom then
				Messaging.PublishInvalidation(self._StoreName, Key)
			end
			return Errors.Passed(Outcome.Lease)
		end

		if Outcome.Tag == "LockedByOther" then
			if SafeEnvelopeLock ~= nil and SafeEnvelopeLock.jobId == "" then
				warn(`[Nexus] MemoryStore says '{Key}' locked by {Outcome.Holder}, but envelope is released. Trusting envelope.`)
				EnvelopeOverride = true
			else
				return Errors.Fail(Errors.New("SessionLocked",
					`Key '{Key}' is locked by server {Outcome.Holder}`,
					{ Key = Key }
				))
			end
		end

		if Outcome.Tag == "ServiceError" then
			warn(`[Nexus] MemoryStore error for '{Key}': {Outcome.Message}. Falling back to envelope.`)
		end
	end

	local EnvelopeResult = self:_AcquireViaEnvelope(Key, CurrentTime, SafeEnvelopeLock)

	if EnvelopeResult.Passed and EnvelopeOverride and self._Circuit:ShouldAttemptCall() then
		local AcquiredLease = (EnvelopeResult :: Types.SuccessResult<Lease>).Value
		pcall(function()
			self._HashMap:SetAsync(Key, AcquiredLease :: any, self._LeaseTimeToLive)
		end)
	end

	return EnvelopeResult
end

function Lock.Release(self : LockManager, Key : string) : Result<boolean>
	self:_StopHeartbeat(Key)

	local WasHolder = self._ActiveLeases[Key] ~= nil
	self._ActiveLeases[Key] = nil

	if WasHolder and self._Circuit:ShouldAttemptCall() then
		local Success, ErrorMessage = pcall(function() : string?
			self._HashMap:RemoveAsync(Key)
			return nil
		end)
		if Success then
			self._Circuit:RecordSuccess()
		else
			if Circuit.IsServiceError(tostring(ErrorMessage)) then
				self._Circuit:RecordServiceFailure()
			end
		end
	end

	return Errors.Passed(true)
end

function Lock.Check(self : LockManager, Key : string) : Result<Lease?>
	if not self._Circuit:ShouldAttemptCall() then
		return Errors.Passed(nil :: Lease?)
	end

	local Success, Result = pcall(function() : unknown?
		return self._HashMap:GetAsync(Key)
	end)

	if not Success then
		if Circuit.IsServiceError(tostring(Result)) then
			self._Circuit:RecordServiceFailure()
		end
		return Errors.Passed(nil :: Lease?)
	end

	self._Circuit:RecordSuccess()
	if Result == nil then
		return Errors.Passed(nil :: Lease?)
	end
	return Errors.Passed((Result :: any) :: Lease?)
end

function Lock.ForceAcquire(self : LockManager, Key : string) : Result<Lease>
	self:_StopHeartbeat(Key)

	local CurrentTime = os.time()
	local ForcedLease : Lease = {
		JobId = SERVER_JOB_ID,
		ExpiresAt = CurrentTime + self._LeaseTimeToLive,
		LeaseVersion = 1,
		RenewedAt = CurrentTime,
	}

	if self._Circuit:ShouldAttemptCall() then
		local Success, ErrorMessage = pcall(function() : string?
			self._HashMap:SetAsync(Key, ForcedLease :: any, self._LeaseTimeToLive)
			return nil
		end)
		if Success then
			self._Circuit:RecordSuccess()
		else
			if Circuit.IsServiceError(tostring(ErrorMessage)) then
				self._Circuit:RecordServiceFailure()
			end
		end
	end

	self._ActiveLeases[Key] = ForcedLease
	self:_StartHeartbeat(Key)

	Messaging.PublishInvalidation(self._StoreName, Key)
	return Errors.Passed(ForcedLease)
end

function Lock.ReleaseAll(self : LockManager) : ()
	-- Snapshot keys; Release mutates _ActiveLeases.
	local Keys : { string } = {}
	for Key, _ in self._ActiveLeases do
		table.insert(Keys, Key)
	end
	for _, Key in Keys do
		self:Release(Key)
	end
end

function Lock.Invalidate(self : LockManager, Key : string) : ()
	self:_StopHeartbeat(Key)
	self._ActiveLeases[Key] = nil
end

function Lock.GetCircuit(self : LockManager) : Circuit.CircuitBreaker
	return self._Circuit
end

function Lock.GetActiveLeases(self : LockManager) : { [string] : Lease }
	return self._ActiveLeases
end

function Lock.ToLockInfo(self : LockManager, Key : string) : LockInfo?
	local ActiveLease = self._ActiveLeases[Key]
	if not ActiveLease then
		return nil
	end
	return {
		jobId = ActiveLease.JobId,
		renewedAt = ActiveLease.RenewedAt,
		lockVersion = ActiveLease.LeaseVersion,
	}
end

function Lock.ToReleasedLockInfo() : LockInfo
	return {
		jobId = "",
		renewedAt = 0,
		lockVersion = 0,
	}
end

function Lock._TryMemoryStore(self : LockManager, Key : string, CurrentTime : number) : AcquireOutcome
	local NewLease : Lease = {
		JobId = SERVER_JOB_ID,
		ExpiresAt = CurrentTime + self._LeaseTimeToLive,
		LeaseVersion = 1,
		RenewedAt = CurrentTime,
	}

	local WasAcquired = false
	local IsLockedByOther = false
	local OtherHolderJobId = ""
	local StolenFromJobId : string? = nil

	local PcallSuccess, PcallError = pcall(function() : string?
		self._HashMap:UpdateAsync(Key, function(Existing : { [string] : unknown }?)
			if Existing == nil then
				WasAcquired = true
				return NewLease :: any
			end

			local RemoteLease = (Existing :: any) :: Lease

			-- Defensive: if the remote entry isn't shaped like a Lease, treat it as empty.
			if typeof(RemoteLease) ~= "table" or typeof(RemoteLease.JobId) ~= "string"
				or typeof(RemoteLease.ExpiresAt) ~= "number"
				or typeof(RemoteLease.LeaseVersion) ~= "number"
			then
				WasAcquired = true
				return NewLease :: any
			end

			if RemoteLease.JobId == SERVER_JOB_ID then
				NewLease.LeaseVersion = RemoteLease.LeaseVersion + 1
				WasAcquired = true
				return NewLease :: any
			end

			-- Add clock-drift tolerance before considering the lease stealable.
			if RemoteLease.ExpiresAt + self._ClockDriftTolerance <= CurrentTime then
				StolenFromJobId = RemoteLease.JobId
				NewLease.LeaseVersion = RemoteLease.LeaseVersion + 1
				WasAcquired = true
				return NewLease :: any
			end

			IsLockedByOther = true
			OtherHolderJobId = RemoteLease.JobId
			return nil
		end, self._LeaseTimeToLive)
		return nil
	end)

	if not PcallSuccess then
		local ErrorString = tostring(PcallError)
		if Circuit.IsServiceError(ErrorString) then
			self._Circuit:RecordServiceFailure()
		end
		return { Tag = "ServiceError", Message = ErrorString }
	end

	self._Circuit:RecordSuccess()

	if WasAcquired then
		return { Tag = "Acquired", Lease = NewLease, StolenFrom = StolenFromJobId }
	end

	if IsLockedByOther then
		return { Tag = "LockedByOther", Holder = OtherHolderJobId }
	end

	return { Tag = "ServiceError", Message = "Unexpected UpdateAsync result" }
end

function Lock._AcquireViaEnvelope(
	self : LockManager,
	Key : string,
	CurrentTime : number,
	EnvelopeLock : LockInfo?
) : Result<Lease>
	if EnvelopeLock == nil then
		local NewLease = self:_CreateEnvelopeLease(CurrentTime)
		self._ActiveLeases[Key] = NewLease
		self:_StartHeartbeat(Key)
		warn(`[Nexus] Acquired '{Key}' via envelope (no existing lock)`)
		return Errors.Passed(NewLease)
	end

	if EnvelopeLock.jobId == "" then
		local NewLease = self:_CreateEnvelopeLease(CurrentTime)
		self._ActiveLeases[Key] = NewLease
		self:_StartHeartbeat(Key)
		return Errors.Passed(NewLease)
	end

	if EnvelopeLock.jobId == SERVER_JOB_ID then
		local NewLease : Lease = {
			JobId = SERVER_JOB_ID,
			ExpiresAt = CurrentTime + self._EnvelopeLeaseTimeToLive,
			LeaseVersion = EnvelopeLock.lockVersion + 1,
			RenewedAt = CurrentTime,
		}
		self._ActiveLeases[Key] = NewLease
		self:_StartHeartbeat(Key)
		return Errors.Passed(NewLease)
	end

	local LockAge = CurrentTime - EnvelopeLock.renewedAt
	if LockAge > self._EnvelopeLeaseTimeToLive + self._ClockDriftTolerance then
		local NewLease : Lease = {
			JobId = SERVER_JOB_ID,
			ExpiresAt = CurrentTime + self._EnvelopeLeaseTimeToLive,
			LeaseVersion = EnvelopeLock.lockVersion + 1,
			RenewedAt = CurrentTime,
		}
		self._ActiveLeases[Key] = NewLease
		self:_StartHeartbeat(Key)
		warn(`[Nexus] Stole stale lease for '{Key}' (age {LockAge}s > {self._EnvelopeLeaseTimeToLive}s)`)
		Messaging.PublishInvalidation(self._StoreName, Key)
		return Errors.Passed(NewLease)
	end

	return Errors.Fail(Errors.New("SessionLocked",
		`Key '{Key}' is locked by {EnvelopeLock.jobId} (envelope, age {LockAge}s)`,
		{ Key = Key }
	))
end

function Lock._CreateEnvelopeLease(self : LockManager, CurrentTime : number) : Lease
	return {
		JobId = SERVER_JOB_ID,
		ExpiresAt = CurrentTime + self._EnvelopeLeaseTimeToLive,
		LeaseVersion = 1,
		RenewedAt = CurrentTime,
	}
end

function Lock._StartHeartbeat(self : LockManager, Key : string) : ()
	self:_StopHeartbeat(Key)

	-- Increment generation so any stale heartbeat thread that resumes after
	-- task.cancel (possible while its UpdateAsync call is in-flight) can detect
	-- it no longer owns this lease and bail without touching remote state.
	local Gen = (self._HeartbeatGeneration[Key] or 0) + 1
	self._HeartbeatGeneration[Key] = Gen

	local HeartbeatThread = task.spawn(function()
		while true do
			task.wait(self._HeartbeatInterval)

			-- Bail if generation has advanced (Released or re-Acquired).
			if self._HeartbeatGeneration[Key] ~= Gen then break end

			local ActiveLease = self._ActiveLeases[Key]
			if not ActiveLease then
				break
			end
			local CurrentTime = os.time()
			local LeaseWasStolen = false
			local StealerJobId : string? = nil

			if self._Circuit:ShouldAttemptCall() then
				-- Watchdog: if the renewal call doesn't return within HeartbeatWatchdog
				-- seconds we fail-open by recording a service failure. This prevents
				-- the heartbeat thread from blocking forever on a stuck MemoryStore call.
				local CallStartTime = os.clock()
				local RenewalSuccess, RenewalError = pcall(function() : string?
					self._HashMap:UpdateAsync(Key, function(Existing : { [string] : unknown }?)
						-- Re-check generation INSIDE the callback: task.cancel may have fired
						-- between the yield and the callback executing.
						if self._HeartbeatGeneration[Key] ~= Gen then
							return Existing  -- leave remote state untouched
						end

						local CurrentActiveLease = self._ActiveLeases[Key]
						if CurrentActiveLease == nil then
							return Existing  -- released while async call was in-flight
						end

						if Existing == nil then
							CurrentActiveLease.ExpiresAt = CurrentTime + self._LeaseTimeToLive
							CurrentActiveLease.RenewedAt = CurrentTime
							CurrentActiveLease.LeaseVersion += 1
							return CurrentActiveLease :: any
						end

						local RemoteLease = (Existing :: any) :: Lease
						if typeof(RemoteLease) ~= "table" or typeof(RemoteLease.JobId) ~= "string" then
							-- Corrupt entry — reclaim it.
							CurrentActiveLease.ExpiresAt = CurrentTime + self._LeaseTimeToLive
							CurrentActiveLease.RenewedAt = CurrentTime
							CurrentActiveLease.LeaseVersion += 1
							return CurrentActiveLease :: any
						end

						if RemoteLease.JobId ~= SERVER_JOB_ID then
							warn(`[Nexus] Lease for '{Key}' stolen by {RemoteLease.JobId}!`)
							LeaseWasStolen = true
							StealerJobId = RemoteLease.JobId
							return Existing
						end

						if RemoteLease.LeaseVersion > CurrentActiveLease.LeaseVersion then
							CurrentActiveLease.LeaseVersion = RemoteLease.LeaseVersion
						end

						CurrentActiveLease.ExpiresAt = CurrentTime + self._LeaseTimeToLive
						CurrentActiveLease.RenewedAt = CurrentTime
						CurrentActiveLease.LeaseVersion += 1
						return CurrentActiveLease :: any
					end, self._LeaseTimeToLive)
					return nil
				end)

				local Elapsed = os.clock() - CallStartTime
				if Elapsed > self._HeartbeatWatchdog then
					warn(`[Nexus] Heartbeat for '{Key}' took {string.format("%.1f", Elapsed)}s (> watchdog {self._HeartbeatWatchdog}s)`)
					self._Circuit:RecordServiceFailure()
				end

				if RenewalSuccess then
					self._Circuit:RecordSuccess()
				else
					if Circuit.IsServiceError(tostring(RenewalError)) then
						self._Circuit:RecordServiceFailure()
					end
				end
			end

			-- Final generation check after the async call returns.
			if self._HeartbeatGeneration[Key] ~= Gen then break end

			if LeaseWasStolen then
				self._ActiveLeases[Key] = nil
				if self._OnStolen and StealerJobId then
					task.spawn(self._OnStolen, Key, StealerJobId)
				end
				break
			end

			local CurrentActiveLease = self._ActiveLeases[Key]
			if CurrentActiveLease then
				CurrentActiveLease.RenewedAt = CurrentTime
			end
		end
	end)

	self._ActiveHeartbeats[Key] = HeartbeatThread
end

function Lock._StopHeartbeat(self : LockManager, Key : string) : ()
	local ExistingThread = self._ActiveHeartbeats[Key]
	if ExistingThread then
		pcall(task.cancel, ExistingThread)
		self._ActiveHeartbeats[Key] = nil
	end
end

return Lock
