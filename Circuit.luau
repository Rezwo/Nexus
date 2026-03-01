--!strict

local Types = require(script.Parent.Types)
local Signal = require(script.Parent.Signal)

type Signal<T...> = Types.Signal<T...>

local Circuit = {}
Circuit.__index = Circuit

export type CircuitState = "Closed" | "Open" | "HalfOpen"

export type CircuitBreaker = typeof(setmetatable({} :: {
	_State : CircuitState,
	_FailureCount : number,
	_SuccessCount : number,
	_LastFailureTime : number,
	_LastStateChangeTime : number,
	_FailureThreshold : number,
	_RecoveryTimeout : number,
	_HalfOpenSuccessThreshold : number,
	_Name : string,
	OnStateChange : Signal<CircuitState, CircuitState>,
}, {} :: { __index : typeof(Circuit) }))

export type CircuitConfiguration = {
	Name : string?,
	FailureThreshold : number?,
	RecoveryTimeout : number?,
	HalfOpenSuccessThreshold : number?,
}

function Circuit.New(Configuration : CircuitConfiguration?) : CircuitBreaker
	local Config : CircuitConfiguration = (Configuration or {}) :: any
	local self = setmetatable({}, Circuit)
	self._State = "Closed" :: CircuitState
	self._FailureCount = 0
	self._SuccessCount = 0
	self._LastFailureTime = 0
	self._LastStateChangeTime = os.clock()
	self._FailureThreshold = Config.FailureThreshold or 5
	self._RecoveryTimeout = Config.RecoveryTimeout or 30
	self._HalfOpenSuccessThreshold = Config.HalfOpenSuccessThreshold or 2
	self._Name = Config.Name or "Circuit"
	self.OnStateChange = Signal.New()
	return self :: any
end

function Circuit.GetState(self : CircuitBreaker) : CircuitState
	if self._State == "Open" then
		if os.clock() - self._LastStateChangeTime >= self._RecoveryTimeout then
			self:_Transition("HalfOpen")
		end
	end
	return self._State
end

function Circuit.ShouldAttemptCall(self : CircuitBreaker) : boolean
	local State = self:GetState()
	return State == "Closed" or State == "HalfOpen"
end

function Circuit.IsOpen(self : CircuitBreaker) : boolean
	return self:GetState() == "Open"
end

function Circuit.RecordSuccess(self : CircuitBreaker) : ()
	local State = self:GetState()
	if State == "HalfOpen" then
		self._SuccessCount += 1
		if self._SuccessCount >= self._HalfOpenSuccessThreshold then
			self:_Transition("Closed")
		end
	elseif State == "Closed" then
		self._FailureCount = 0
	end
end

function Circuit.RecordServiceFailure(self : CircuitBreaker) : ()
	local State = self:GetState()
	self._LastFailureTime = os.clock()

	if State == "HalfOpen" then
		self._SuccessCount = 0
		self:_Transition("Open")
	elseif State == "Closed" then
		self._FailureCount += 1
		if self._FailureCount >= self._FailureThreshold then
			self:_Transition("Open")
		end
	end
end

function Circuit.IsServiceError(ErrorString : string) : boolean
	local LowerCase = string.lower(ErrorString)
	if string.find(LowerCase, "internal") then
		return true
	end
	if string.find(LowerCase, "unavailable") then
		return true
	end
	if string.find(LowerCase, "timeout") then
		return true
	end
	if string.find(LowerCase, "throttle") then
		return true
	end
	if string.find(LowerCase, "too many requests") then
		return true
	end
	if string.find(LowerCase, "service") then
		return true
	end
	if string.find(LowerCase, "500") then
		return true
	end
	if string.find(LowerCase, "502") then
		return true
	end
	if string.find(LowerCase, "503") then
		return true
	end
	return false
end

function Circuit.ForceState(self : CircuitBreaker, NewState : CircuitState) : ()
	self:_Transition(NewState)
end

function Circuit.Reset(self : CircuitBreaker) : ()
	self._FailureCount = 0
	self._SuccessCount = 0
	self:_Transition("Closed")
end

function Circuit.GetInfo(self : CircuitBreaker) : {
	Name : string,
	State : CircuitState,
	FailureCount : number,
	SuccessCount : number,
	LastFailureTime : number,
	TimeSinceStateChange : number,
}
	return {
		Name = self._Name,
		State = self:GetState(),
		FailureCount = self._FailureCount,
		SuccessCount = self._SuccessCount,
		LastFailureTime = self._LastFailureTime,
		TimeSinceStateChange = os.clock() - self._LastStateChangeTime,
	}
end

function Circuit.Destroy(self : CircuitBreaker) : ()
	self.OnStateChange:Destroy()
end

function Circuit._Transition(self : CircuitBreaker, NewState : CircuitState) : ()
	local OldState = self._State
	if OldState == NewState then
		return
	end

	self._State = NewState
	self._LastStateChangeTime = os.clock()

	if NewState == "Closed" then
		self._FailureCount = 0
		self._SuccessCount = 0
	elseif NewState == "HalfOpen" then
		self._SuccessCount = 0
	end
	
	warn(`[Nexus:{self._Name}] Circuit {OldState} → {NewState}`)
	self.OnStateChange:Fire(OldState, NewState)
end

return Circuit
