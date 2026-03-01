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

local LEASE_TIME_TO_LIVE = 60
local HEARTBEAT_INTERVAL = 15
local ENVELOPE_LEASE_TIME_TO_LIVE = 120

local SERVER_JOB_ID = game.JobId ~= "" and game.JobId or HttpService:GenerateGUID(false)

export type Lease = {
	JobId : string,
	ExpiresAt : number,
	LeaseVersion : number,
	RenewedAt : number,
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
	_Circuit : Circuit.CircuitBreaker,
}, {} :: { __index : typeof(Lock) }))

function Lock.New(StoreName : string) : LockManager
	local self = setmetatable({}, Lock)
	self._HashMap = MemoryStoreService:GetHashMap(`Nexus_Lease_{StoreName}`)
	self._StoreName = StoreName
	self._ActiveHeartbeats = {}
	self._ActiveLeases = {}
	self._Circuit = Circuit.New({
		Name = `MemoryStore:{StoreName}`,
		FailureThreshold = 3,
		RecoveryTimeout = 30,
		HalfOpenSuccessThreshold = 2,
	})
	return self :: any
end

function Lock.Acquire(
	self : LockManager,
	Key : string,
	EnvelopeLock : LockInfo?
) : Result<Lease>
	local CurrentTime = os.time()
	local EnvelopeOverride = false

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

			if EnvelopeLock and EnvelopeLock.jobId == "" then
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

	local EnvelopeResult = self:_AcquireViaEnvelope(Key, CurrentTime, EnvelopeLock)

	if EnvelopeResult.Passed and EnvelopeOverride and self._Circuit:ShouldAttemptCall() then
		local AcquiredLease = (EnvelopeResult :: Types.SuccessResult<Lease>).Value
		pcall(function()
			self._HashMap:SetAsync(Key, AcquiredLease :: any, LEASE_TIME_TO_LIVE)
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
		ExpiresAt = CurrentTime + LEASE_TIME_TO_LIVE,
		LeaseVersion = 1,
		RenewedAt = CurrentTime,
	}

	if self._Circuit:ShouldAttemptCall() then
		local Success, ErrorMessage = pcall(function() : string?
			self._HashMap:SetAsync(Key, ForcedLease :: any, LEASE_TIME_TO_LIVE)
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
	for Key, _ in self._ActiveLeases do
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
		ExpiresAt = CurrentTime + LEASE_TIME_TO_LIVE,
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

			if RemoteLease.JobId == SERVER_JOB_ID then
				NewLease.LeaseVersion = RemoteLease.LeaseVersion + 1
				WasAcquired = true
				return NewLease :: any
			end

			if RemoteLease.ExpiresAt <= CurrentTime then
				StolenFromJobId = RemoteLease.JobId
				NewLease.LeaseVersion = RemoteLease.LeaseVersion + 1
				WasAcquired = true
				return NewLease :: any
			end

			IsLockedByOther = true
			OtherHolderJobId = RemoteLease.JobId
			return nil
		end, LEASE_TIME_TO_LIVE)
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
			ExpiresAt = CurrentTime + ENVELOPE_LEASE_TIME_TO_LIVE,
			LeaseVersion = EnvelopeLock.lockVersion + 1,
			RenewedAt = CurrentTime,
		}
		self._ActiveLeases[Key] = NewLease
		self:_StartHeartbeat(Key)
		return Errors.Passed(NewLease)
	end

	local LockAge = CurrentTime - EnvelopeLock.renewedAt
	if LockAge > ENVELOPE_LEASE_TIME_TO_LIVE then
		local NewLease : Lease = {
			JobId = SERVER_JOB_ID,
			ExpiresAt = CurrentTime + ENVELOPE_LEASE_TIME_TO_LIVE,
			LeaseVersion = EnvelopeLock.lockVersion + 1,
			RenewedAt = CurrentTime,
		}
		self._ActiveLeases[Key] = NewLease
		self:_StartHeartbeat(Key)
		warn(`[Nexus] Stole stale lease for '{Key}' (age {LockAge}s > {ENVELOPE_LEASE_TIME_TO_LIVE}s)`)
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
		ExpiresAt = CurrentTime + ENVELOPE_LEASE_TIME_TO_LIVE,
		LeaseVersion = 1,
		RenewedAt = CurrentTime,
	}
end

function Lock._StartHeartbeat(self : LockManager, Key : string) : ()
	self:_StopHeartbeat(Key)

	local HeartbeatThread = task.spawn(function()
		while true do
			task.wait(HEARTBEAT_INTERVAL)

			local ActiveLease = self._ActiveLeases[Key]
			if not ActiveLease then
				break
			end
			local CurrentTime = os.time()
			local LeaseWasStolen = false

			if self._Circuit:ShouldAttemptCall() then
				local RenewalSuccess, RenewalError = pcall(function() : string?
					self._HashMap:UpdateAsync(Key, function(Existing : { [string] : unknown }?)
						if Existing == nil then
							ActiveLease.ExpiresAt = CurrentTime + LEASE_TIME_TO_LIVE
							ActiveLease.RenewedAt = CurrentTime
							ActiveLease.LeaseVersion += 1
							return ActiveLease :: any
						end

						local RemoteLease = (Existing :: any) :: Lease
						if RemoteLease.JobId ~= SERVER_JOB_ID then
							warn(`[Nexus] Lease for '{Key}' stolen by {RemoteLease.JobId}!`)
							LeaseWasStolen = true
							return Existing
						end

						if RemoteLease.LeaseVersion > ActiveLease.LeaseVersion then
							ActiveLease.LeaseVersion = RemoteLease.LeaseVersion
						end

						ActiveLease.ExpiresAt = CurrentTime + LEASE_TIME_TO_LIVE
						ActiveLease.RenewedAt = CurrentTime
						ActiveLease.LeaseVersion += 1
						return ActiveLease :: any
					end, LEASE_TIME_TO_LIVE)
					return nil
				end)

				if RenewalSuccess then
					self._Circuit:RecordSuccess()
				else
					if Circuit.IsServiceError(tostring(RenewalError)) then
						self._Circuit:RecordServiceFailure()
					end
				end
			end

			if LeaseWasStolen then
				self._ActiveLeases[Key] = nil
				break
			end

			ActiveLease.RenewedAt = CurrentTime
			self._ActiveLeases[Key] = ActiveLease
		end
	end)

	self._ActiveHeartbeats[Key] = HeartbeatThread
end

function Lock._StopHeartbeat(self : LockManager, Key : string) : ()
	local ExistingThread = self._ActiveHeartbeats[Key]
	if ExistingThread then
		task.cancel(ExistingThread)
		self._ActiveHeartbeats[Key] = nil
	end
end

return Lock
