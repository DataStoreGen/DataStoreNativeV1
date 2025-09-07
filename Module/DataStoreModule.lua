--!strict
local ModuleCleaner = require(script.Parent:FindFirstChild('ModuleCleaner'))
local Cleaner = ModuleCleaner.new()

local DataStore = game:GetService('DataStoreService')
local HttpService = game:GetService('HttpService')
local RunService = game:GetService('RunService')

local PlayersData = require(script.Parent.PlayersData)

local eventListen = {}
local asyncQueue = {}
local queueMutex = false
local config = {
	max = 30,
	retry = 0.3,
	versionAsync = 'v1.0',
	logLevel = 'Info'
}

function enqueueAsync(func)
	while queueMutex do task.wait(0.01) end
	queueMutex = true
	table.insert(asyncQueue, func)
	queueMutex = false
end

function logMessage(level, message)
	if level == 'Error' or config.logLevel == 'Debug' or (config.logLevel == 'Info' and level ~= 'Debug') then
		local levelMessage = string.format('[ServiceModule] [%s] %s', level, message)
		if level == 'Error' then
			error(levelMessage)
		elseif level == 'Warn' then
			warn(levelMessage)
		else
			print(levelMessage)
		end
	end
end

function dispatchEvent(event, ...)
	if eventListen[event] then
		for _, listener in ipairs(eventListen[event]) do
			coroutine.wrap(listener)(...)
		end
	end
end

function retryAsync(func)
	local retries = 0
	while retries < config.max do
		local success, result = pcall(func)
		if success then return result else
			retries += 1
			logMessage('Warn', string.format('Retrying ... (%d/%d) %s',  retries, config.max, result))
		end
	end
	error(string.format("Failed after %d retries", config.max))
end

local function processQueue()
	while true do
		if #asyncQueue > 0 then
			local func
			while queueMutex do task.wait(0.01) end
			queueMutex = true
			func = table.remove(asyncQueue, 1)
			queueMutex = false
			local success, err = pcall(func)
			if not success then
				logMessage('Error', string.format("Queuing process error: %s", err))
				dispatchEvent('queueError', err)
			else
				dispatchEvent('queueProcessed', func)
			end
		end
		task.wait(0.1)
	end
end

coroutine.wrap(processQueue)()

export type Config = {
	Name: string,
	Scope: string?,
	DataStoreInstance: Instance?,
	CacheEnabled: boolean,
	CacheTTL: number?,
	LockTTL: number,
}
export type LockEntry = {
	SessionId: string,
	Expiration: number
}

export type DataStoreModule = {
	GetAsync: (self: DataStoreModule, key: string, player: Player) -> PlayersData.PlayerData,
	SetAsync: (self: DataStoreModule, key: string, data: any, userIds: {any}?, option: DataStoreSetOptions?)->(boolean, string?),
	UpdateAsync: (self: DataStoreModule, key: string, callBack: (oldData: any) -> any) ->(boolean, string?),
	RemoveAsync: (self: DataStoreModule, key: string) -> (boolean, string?),
	AcquireLock: (self: DataStoreModule, key: string) -> (boolean, string?),
	ReleaseLock: (self: DataStoreModule, key: string) -> (boolean, string?),
	IsLocked: (self: DataStoreModule, key: string) -> boolean,
	CloneTemplate: (self: DataStoreModule, originalData: {[string]: any}) -> PlayersData.PlayerData
}

export type UserData = PlayersData.PlayerData

local DataStoreMod = {}
DataStoreMod.__index = DataStoreMod

function DataStoreMod.new(config: Config): DataStoreModule
	assert(config.Name, 'a DataStore name must be well named')
	local self = setmetatable({}, DataStoreMod)
	self.DataStore = DataStore:GetDataStore(config.Name, config.Scope, config.DataStoreInstance)
	self.CacheEnabled = config.CacheEnabled or false
	self.CacheTTL = config.CacheTTL or 60
	self.LockTTL = config.LockTTL or 30
	self.Cache = {} :: {[string]: {Value: any, Expiration: number}}	
	self.SessionId = HttpService:GenerateGUID(false)
	self.CloneTemplate = function(originalData: {[string]: any}): {[string]: any}
		return ModuleCleaner.CloneTemplate(originalData)
	end
	self.PlayersData = PlayersData
	return self
end

function DataStoreMod:_GetLockKey(key: string): string
	return key .. '_lock'
end

function DataStoreMod:_GetFromCache(key: string): any
	if not self.CacheEnabled then
		return nil
	end
	local cacheEntry = self.Cache and self.Cache[key]
	if self.CacheEnabled and cacheEntry then
		return cacheEntry.Value
	else
		self.Cache[key] = nil
		return nil
	end
end

function DataStoreMod:_SaveToCache(key: string, value: any)
	if self.CacheEnabled then
		self.Cache[key] = {
			Value = value,
			Expiration = os.time()+ self.CacheTTL
		}
	end
end

function DataStoreMod:IsLocked(key: string): boolean
	assert(key ~='', `key can't be empty`)
	local lockKey = self:_GetLockKey(key)
	local success, result = pcall(function()
		return self.DataStore:GetAsync(lockKey)
	end)
	result = Cleaner:RegisterTable('GetAsync', result)
	if success and result then
		return result.Expiration > os.time()
	else
		return false
	end
end

function DataStoreMod:AcquireLock(key: string, player: Player): (boolean, string?)
	assert(key ~= '', "Key can't be empty")
	local lockKey = self:_GetLockKey(key)
	local success, err = pcall(function()
		self.DataStore:UpdateAsync(lockKey, function(currentLock: LockEntry)
			if currentLock and currentLock.Expiration > os.time() then
				if currentLock.SessionId == self.SessionId then
					currentLock.Expiration = os.time() + self.LockTTL
					return currentLock
				end
				return nil
			else
				return Cleaner:RegisterTable('SessionLock',{
					SessionId = self.SessionId,
					Expiration = os.time() + self.LockTTL
				})
			end
		end)
	end)
	if success then
		return true, nil
	else
		warn(`Failed to lock session because: {err}`)
		return false, err
	end
end

function DataStoreMod:ReleaseLock(key: string): (boolean, string?)
	assert(key ~= '', `key can't be empty`)
	local lockKey = self:_GetLockKey(key)
	local success, err = pcall(function()
		self.DataStore:UpdateAsync(lockKey, function(currentLock: LockEntry?)
			if currentLock and currentLock.SessionId == self.SessionId then return nil else
				return currentLock
			end
		end)
	end)
	if success then
		return true, nil else
		warn('Failed to get lock for key:', key, err)
		return false, err
	end
end

function DataStoreMod:Budget(BudgetType: Enum.DataStoreRequestType)
	local current = DataStore:GetRequestBudgetForRequestType(BudgetType)
	while current < 1 do
		task.wait(5)
		current = DataStore:GetRequestBudgetForRequestType(BudgetType)
	end
end

function DataStoreMod:SetAsync(key: string, data: any, canBindData, userIds: {any}?, option: DataStoreSetOptions?): (boolean, string?)
	assert(key ~='', `key can't be empty`)
	assert(data ~= nil, `data can't be nil since it will not save`)
	local success, err
	repeat
		if not canBindData then self:Budget(Enum.DataStoreRequestType.SetIncrementAsync) end
		success, err = pcall(function()
			return retryAsync(function()
				return self.DataStore:SetAsync(key, data, userIds, option)
			end)
		end)
		if success then
			return true, nil
		else
			warn(`Failed to update to: {key}`)
			return false, err
		end
	until success
	if not success then
		logMessage('Warn', 'Failed to save to: ' .. key)
	end
end

function DataStoreMod:UpdateAsync(key, callBack: (oldData: any?)-> any, canBind: boolean?): (boolean, string?)
	assert(key ~='', `key can't be empty`)
	assert(type(callBack) == 'function', `callBack must be a function u weirdo`)
	local success, result
	repeat
		if not canBind then self:Budget(Enum.DataStoreRequestType.UpdateAsync) end
		success, result = pcall(function()
			enqueueAsync(function()
				retryAsync(function()
					return self.DataStore:UpdateAsync(key, callBack)
				end)
			end)
		end)
	until success
	if success then
		return true, result
	else
		warn(`Failed to update to: {key}, {result}`)
		return false, result
	end
end

function DataStoreMod:GetAsync(key: string): PlayersData.PlayerData?
	assert(key ~= '', `key can't be empty`)
	local success, result = pcall(function()
		return retryAsync(function()
			return self.DataStore:GetAsync(key)
		end)
	end)
	if success then
		if result == nil then
			result = self.CloneTemplate(PlayersData)
		end
		self:_SaveToCache(key, result)
		return result
	else
		return self.CloneTemplate(PlayersData)
	end
end

function DataStoreMod:MergeToCurrent(data)
	local function recursiveMerge(template, target)
		for key, value in pairs(template) do
			if type(value) == 'table' then
				target[key] = target[key] or {}
				recursiveMerge(value, target[key])
			else
				if target[key] == nil then
					target[key] = value
				end
			end
		end
	end
	recursiveMerge(PlayersData, data)
	return data
end

function DataStoreMod:CleanData(data)
	local function recursiveClean(template, target)
		for key in pairs(target) do
			if template[key] == nil then
				target[key] = nil
			elseif type(template[key]) == 'table' and type(target[key]) == 'table' then
				recursiveClean(template[key], target[key])
			end
		end
	end
	recursiveClean(PlayersData, data)
end

function DataStoreMod.GetSessionFromStore(key, self)
	local session = self.Session[key]
	return session
end

function DataStoreMod.GetPlayersData(): PlayersData.PlayerData
	return PlayersData
end

Cleaner:RegisterTable('DataStoreModule', DataStoreMod)

return DataStoreMod