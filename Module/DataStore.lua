local ModuleCleaner = require(script.Parent:FindFirstChild('ModuleCleaner')).new()
local DataStore = require(script.Parent.DataStoreModule)
local SessionStore = require(script.Parent.SessionStore)
local RunService = game:GetService('RunService')
local DataStoreModule = {}
DataStoreModule.__index = DataStoreModule

function DataStoreModule.new(config: DataStore.Config)
	local self = {}
	self.DataStore = DataStore.new(config)
	self.Session = SessionStore.Session
	setmetatable(self, DataStoreModule)
	return self
end

function DataStoreModule:IsSessionLocked(key)
	return self.DataStore:IsLocked(key)
end

function DataStoreModule:AcquireLockSession(key: string)
	return self.DataStore:AcquireLock(key)
end

function DataStoreModule:ReleaseLockSession(key)
	return self.DataStore:ReleaseLock(key)
end

function DataStoreModule:GetAsync(key: string, player: Player): DataStore.UserData
	local lockSuccess, lockErr = self:AcquireLockSession(key)
	if self:IsSessionLocked(key) and not lockSuccess then
		player:Kick('Your session is already active in another server')
		return nil
	end
	local Session = self.Session
	local success, result = pcall(function()
		return self.DataStore:GetAsync(key)
	end)
	if not result then
		result = self.DataStore:_GetFromCache(key)
	end
	if success then
		self.DataStore:MergeToCurrent(result)
		self.DataStore:CleanData(result)
		Session[key] = result
	end
	self:ReleaseLockSession(key)
	return Session[key]
end

function DataStoreModule:GetSession(key)
	local session = self.Session[key]
	if not session then return end
	return session
end

function DataStoreModule:SetAsync(key: string, canBindData: boolean?, userIds: {number}?, option: DataStoreSetOptions?)
	local lockSuccess, lockErr = self:AcquireLockSession(key)
	if not lockSuccess then
		warn('failed to get lock', lockErr)
	end
	local session = self:GetSession(key)
	if not session then return end
	local success, err = self.DataStore:SetAsync(key, session, canBindData, userIds, option)
	if not success then
		warn(`Failed to save data because of: {err}`)
	end
	self:ReleaseLockSession(key)
	return success, err
end

function DataStoreModule:UpdateAsync(key: string, canBind: boolean?)
	local lockSuccess, lockErr = self:AcquireLockSession(key)
	if not lockSuccess then
		warn('failed to get lock', lockErr)
	end
	local session = self:GetSession(key)
	if not session then return end
	local success, err = pcall(function()
		return self.DataStore:UpdateAsync(key, function(oldData)
			return session
		end, canBind)
	end)
	if not success then
		warn(`Failed to save data because of: {err}`)
	end
	self:ReleaseLockSession(key)
	return success, err
end

function DataStoreModule.GetSessionData(key): DataStore.UserData
	local session = DataStore.GetSessionFromStore(key, SessionStore)
	if not session then return end
	ModuleCleaner:RegisterTable('SessionData', session)
	return session
end

function DataStoreModule:OnLeaveSet(key, userIds: {any}?, options: DataStoreSetOptions?)
	self:SetAsync(key, false, userIds, options)
	self.Session[key] = nil
end

function DataStoreModule:OnLeaveUpdate(key)
	self:UpdateAsync(key, false)
	self.Session[key] = nil
end

function DataStoreModule:BindOnSet(key, players: Players)
	if RunService:IsServer() then return task.wait(2) end
	local bind = Instance.new('BindableEvent')
	local allPlayers = players:GetPlayers()
	local allCurrent = #allPlayers
	for _, player in pairs(allPlayers) do
		task.spawn(function()
			key = key .. player.UserId
			self:SetAsync(key, true)
			allCurrent -= 1
			if allCurrent <= 0 then bind:Fire() end
		end)
	end
	bind.Event:Wait()
end

function DataStoreModule:BindOnUpdate(key: string, players: Players)
	if RunService:IsServer() then return task.wait(2) end
	local bind = Instance.new('BindableEvent')
	local allPlayers = players:GetPlayers()
	local allCurrent = #allPlayers
	for _, player in pairs(allPlayers) do
		task.spawn(function()
			key = key .. player.UserId
			self:UpdateAsync(key, true)
			allCurrent -= 1
			if allCurrent <= 0 then bind:Fire() end
		end)
	end
	bind.Event:Wait()
end

function DataStoreModule.SetKey(player: Player)
	return 'Player_' .. player.UserId
end

function DataStoreModule:AutoSaveSet(Players: Players)
	task.spawn(function()
		while task.wait(math.random(180, 300)) do
			for _, players in pairs(Players:GetPlayers()) do
				local key = 'Player_' .. players.UserId
				local success, err = pcall(self.OnLeaveSet, self, key)
				if not success then
					warn('Failed to auto save', err)
				else
					print('Saving in success')
				end	
			end
		end
	end)
end

function DataStoreModule:AutoSaveUpdate(Players: Players)
	while task.wait(math.random(180, 300)) do
		for _, players in pairs(Players:GetPlayers()) do
			local key = 'Player_' .. players.UserId
			local success, err = pcall(self.OnLeaveUpdate, self, key)
			if not success then
				warn('Failed to auto save', err)
			else
				print('Saving in success')
			end	
		end
	end
end

export type Folder = {
	Name: string,
	Parent: any?,
}

function DataStoreModule.CreateFolder(folderName: string, parent: any): Folder
	local folder = Instance.new('Folder')
	folder.Name = folderName
	folder.Parent = parent
	return folder
end

type valueType = 'NumberValue'|'StringValue'|'BoolValue'|'IntValue'
export type Value = {
	Name: string,
	Value: number|string|boolean,
	Parent: Instance?
}

function DataStoreModule.CreateValue(valueIns: valueType, valueName: string, valueParent: any): Value
	local value = Instance.new(valueIns)
	value.Name = valueName
	value.Parent = valueParent
	return value
end

ModuleCleaner:RegisterTable('DataStore', DataStoreModule)

return DataStoreModule