--!strict

local Errors = require(script.Parent.Errors)
local Types = require(script.Parent.Types)
local Validation = require(script.Parent.Validation)

type Result<Value> = Types.Result<Value>
type NexusError = Types.NexusError
type TransactionConfiguration = Types.TransactionConfiguration
type TransactionResult = Types.TransactionResult
type TransactionRollbackStatus = Types.TransactionRollbackStatus

local Transaction = {}
Transaction.__index = Transaction

type TransactionState = "Building" | "Executing" | "Committed" | "RolledBack" | "Failed" | "PartiallyCommitted"

type Participant = {
	StoreReference : unknown,
	Key : string,
	Transform : (Data : unknown) -> unknown?,
	Snapshot : unknown?,
	SnapshotSet : boolean,
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
	assert(typeof(Key) == "string" and #Key > 0, "Transaction participant key must be a non-empty string")
	assert(typeof(Transform) == "function", "Transaction participant transform must be a function")

	for _, Existing in self._Participants do
		if Existing.StoreReference == StoreReference and Existing.Key == Key then
			error(`Duplicate participant for key '{Key}' in transaction`, 2)
		end
	end

	table.insert(self._Participants, {
		StoreReference = StoreReference,
		Key = Key,
		Transform = Transform,
		Snapshot = nil,
		SnapshotSet = false,
		Committed = false,
	})
end

function Transaction.GetParticipants(self : TransactionObject) : { {
	StoreReference : unknown,
	Key : string,
	Transform : (Data : unknown) -> unknown?,
	Snapshot : unknown?,
	SnapshotSet : boolean,
	Committed : boolean,
} }
	return self._Participants
end

function Transaction.SetSnapshot(self : TransactionObject, Index : number, Snapshot : unknown) : ()
	local Target = self._Participants[Index]
	assert(Target, `Transaction.SetSnapshot: no participant at index {Index}`)
	Target.Snapshot = Snapshot
	Target.SnapshotSet = true
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
	assert(self._State == "Building", "Transaction has already been executed")

	-- Pre-validate: every participant must have a snapshot recorded, otherwise
	-- rollback would be impossible if an earlier commit needed to be undone.
	for Index, Participant in self._Participants do
		if not Participant.SnapshotSet then
			self._State = "Failed"
			local SetupError = Errors.New("Internal",
				`Transaction participant at index {Index} (key '{Participant.Key}') has no snapshot set; cannot execute safely`,
				{ Key = Participant.Key }
			)
			return {
				Passed = false,
				Error = SetupError,
				Results = {},
				Rollbacks = nil,
				PartiallyCommitted = false,
			}
		end
	end

	if #self._Participants == 0 then
		self._State = "Committed"
		return {
			Passed = true,
			Results = {},
			Rollbacks = nil,
			PartiallyCommitted = false,
		}
	end

	self._State = "Executing"

	local Results : { [string] : Result<boolean> } = {}
	local AllSucceeded = true
	local FirstError : NexusError? = nil

	for ParticipantIndex, CurrentParticipant in self._Participants do
		local UpdateSuccess, UpdateOrError = pcall(
			UpdateKey,
			CurrentParticipant.StoreReference,
			CurrentParticipant.Key,
			CurrentParticipant.Transform
		)

		local UpdateResult : Result<unknown>
		if UpdateSuccess then
			UpdateResult = UpdateOrError :: Result<unknown>
		else
			UpdateResult = Errors.Fail(Errors.New("Internal",
				`Transaction UpdateKey threw for key '{CurrentParticipant.Key}': {tostring(UpdateOrError)}`,
				{ Key = CurrentParticipant.Key }
			))
		end

		if UpdateResult.Passed then
			CurrentParticipant.Committed = true
			Results[CurrentParticipant.Key] = Errors.Passed(true)
		else
			AllSucceeded = false
			local FailureError = (UpdateResult :: Types.ErrorResult).Error
			Results[CurrentParticipant.Key] = Errors.Fail(FailureError)
			if FirstError == nil then
				FirstError = FailureError
			end

			local Rollbacks : { TransactionRollbackStatus } = {}
			local RollbackFailureCount = 0
			local AnyRolledBack = false

			for RollbackIndex = ParticipantIndex - 1, 1, -1 do
				local PreviousParticipant = self._Participants[RollbackIndex]
				if PreviousParticipant.Committed and PreviousParticipant.SnapshotSet then
					AnyRolledBack = true

					local RollbackSuccess, RollbackOrError = pcall(
						RollbackKey,
						PreviousParticipant.StoreReference,
						PreviousParticipant.Key,
						PreviousParticipant.Snapshot
					)

					local RollbackResult : Result<boolean>
					if RollbackSuccess then
						RollbackResult = RollbackOrError :: Result<boolean>
					else
						RollbackResult = Errors.Fail(Errors.New("Internal",
							`RollbackKey threw for key '{PreviousParticipant.Key}': {tostring(RollbackOrError)}`,
							{ Key = PreviousParticipant.Key }
						))
					end

					if RollbackResult.Passed then
						table.insert(Rollbacks, {
							Key = PreviousParticipant.Key,
							Succeeded = true,
							Error = nil,
						} :: TransactionRollbackStatus)
					else
						RollbackFailureCount += 1
						local RollbackError = (RollbackResult :: Types.ErrorResult).Error
						warn(`[Nexus] Transaction rollback failed for key '{PreviousParticipant.Key}': {RollbackError.Message}`)
						table.insert(Rollbacks, {
							Key = PreviousParticipant.Key,
							Succeeded = false,
							Error = RollbackError,
						} :: TransactionRollbackStatus)
					end
				end
			end

			if AnyRolledBack then
				self._State = if RollbackFailureCount > 0 then "PartiallyCommitted" else "RolledBack"
			else
				self._State = "Failed"
			end

			return {
				Passed = false,
				Error = FirstError,
				Results = Results,
				Rollbacks = if #Rollbacks > 0 then Rollbacks else nil,
				PartiallyCommitted = RollbackFailureCount > 0,
			}
		end
	end

	self._State = "Committed"
	return {
		Passed = true,
		Results = Results,
		Rollbacks = nil,
		PartiallyCommitted = false,
	}
end

return Transaction
