--!strict

local Errors = require(script.Parent.Errors)
local Types = require(script.Parent.Types)
local Validation = require(script.Parent.Validation)

type Result<Value> = Types.Result<Value>
type NexusError = Types.NexusError
type TransactionConfiguration = Types.TransactionConfiguration
type TransactionResult = Types.TransactionResult

local Transaction = {}
Transaction.__index = Transaction

type TransactionState = "Building" | "Executing" | "Committed" | "RolledBack" | "Failed"

type Participant = {
	StoreReference : unknown,
	Key : string,
	Transform : (Data : unknown) -> unknown?,
	Snapshot : unknown?,
	Committed : boolean,
}

export type TransactionObject = typeof(setmetatable({} :: {
	_Participants : { Participant },
	_Configuration : TransactionConfiguration,
	_State : TransactionState,
}, {} :: { __index : typeof(Transaction) }))

function Transaction.New(Configuration : TransactionConfiguration?) : TransactionObject
	local self = setmetatable({}, Transaction)
	self._Participants = {}
	self._Configuration = Configuration or { Timeout = 30, MaxRetries = 3 }
	self._State = "Building"
	return self :: any
end

function Transaction.Add(
	self : TransactionObject,
	StoreReference : unknown,
	Key : string,
	Transform : (Data : unknown) -> unknown?
) : ()
	assert(self._State == "Building", "Cannot add participants after transaction has started")

	table.insert(self._Participants, {
		StoreReference = StoreReference,
		Key = Key,
		Transform = Transform,
		Snapshot = nil,
		Committed = false,
	})
end

function Transaction.GetParticipants(self : TransactionObject) : { {
	StoreReference : unknown,
	Key : string,
	Transform : (Data : unknown) -> unknown?,
	Snapshot : unknown?,
	Committed : boolean,
} }
	return self._Participants
end

function Transaction.SetSnapshot(self : TransactionObject, Index : number, Snapshot : unknown) : ()
	self._Participants[Index].Snapshot = Snapshot
end

function Transaction.MarkCommitted(self : TransactionObject, Index : number) : ()
	self._Participants[Index].Committed = true
end

function Transaction.GetConfiguration(self : TransactionObject) : TransactionConfiguration
	return self._Configuration
end

function Transaction.SetState(self : TransactionObject, State : TransactionState) : ()
	self._State = State
end

function Transaction.GetState(self : TransactionObject) : string
	return self._State
end

function Transaction.ValidateStrict(
	self : TransactionObject,
	GetData : (StoreReference : unknown, Key : string) -> unknown?
) : Result<boolean>
	for _, CurrentParticipant in self._Participants do
		local ParticipantData = GetData(CurrentParticipant.StoreReference, CurrentParticipant.Key)
		if ParticipantData == nil then
			return Errors.Fail(Errors.New("SessionNotFound",
				`Session for key '{CurrentParticipant.Key}' not found in transaction`,
				{ Key = CurrentParticipant.Key }
			))
		end

		local PurityResult = Validation.ValidateTransformPurity(
			CurrentParticipant.Transform :: any,
			ParticipantData
		)
		if not PurityResult.Passed then
			return Errors.Fail((PurityResult :: Types.ErrorResult).Error)
		end
	end

	return Errors.Passed(true)
end

function Transaction.ExecuteOptimistic(
	self : TransactionObject,
	UpdateKey : (StoreReference : unknown, Key : string, Transform : (Data : unknown) -> unknown?) -> Result<unknown>,
	RollbackKey : (StoreReference : unknown, Key : string, Snapshot : unknown) -> Result<boolean>
) : TransactionResult
	self._State = "Executing"

	local Results : { [string] : Result<boolean> } = {}
	local AllSucceeded = true

	for ParticipantIndex, CurrentParticipant in self._Participants do
		local UpdateResult = UpdateKey(
			CurrentParticipant.StoreReference,
			CurrentParticipant.Key,
			CurrentParticipant.Transform
		)

		if UpdateResult.Passed then
			CurrentParticipant.Committed = true
			Results[CurrentParticipant.Key] = Errors.Passed(true)
		else
			AllSucceeded = false
			Results[CurrentParticipant.Key] = Errors.Fail((UpdateResult :: Types.ErrorResult).Error)


			local AnyRolledBack = false
			for RollbackIndex = 1, ParticipantIndex - 1 do
				local PreviousParticipant = self._Participants[RollbackIndex]
				if PreviousParticipant.Committed and PreviousParticipant.Snapshot ~= nil then
					AnyRolledBack = true
					local RollbackResult = RollbackKey(
						PreviousParticipant.StoreReference,
						PreviousParticipant.Key,
						PreviousParticipant.Snapshot
					)
					if not RollbackResult.Passed then
						warn(`[Nexus] Transaction rollback failed for key '{PreviousParticipant.Key}'`)
					end
				end
			end


			self._State = if AnyRolledBack then "RolledBack" else "Failed"
			break
		end
	end

	if AllSucceeded then
		self._State = "Committed"
	end

	return {
		Passed = AllSucceeded,
		Results = Results,
	}
end

return Transaction
