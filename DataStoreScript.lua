local Players = game:GetService('Players')
local SSS = script.Parent
local Module = SSS:FindFirstChild('Module')
local DataStore = require(Module:FIndFirstChild('DataStore'))
local PlayersDataStore = DataStore.new({Name = 'PlayerData1', CacheEnabled = true, LockTTL = 30})

function createStats(player: Player)
	local key = DataStore.SetKey(player)
	local ls = Instance.new('Folder')
	ls.Name = 'leaderstats'
	local PlusData = Instance.new('Folder')
	PlusData.Name = 'PlusData'
	local Clicks = Instance.new('NumberValue')
	Clicks.Name = 'Clicks'
	local ClickPlus = Instance.new('NumberValue')
	ClickPlus.Name = 'ClickPlus'
	local oldData = PlayersDataStore:GetAsync(key, player)
	if oldData then
		Clicks.Value = oldData.Clicks
		ClickPlus.Value = oldData.ClickPlus
	else
		Clicks.Value = 0
		ClickPlus.Value = 1
	end
	Clicks.Parent = ls
	ClickPlus.Parent = PlusData
	ls.Parent = player
	PlusData.Parent = player
end

Players.PlayerAdded:Connect(function(player)
	createStats(player)
end)

Players.PlayerRemoving:Connect(function(player)
	PlayersDataStore:OnLeaveUpdate(DataStore.SetKey(player))
end)

game:BindToClose(function()
	local key = 'Player_'
	PlayersDataStore:BindOnUpdate(key, Players)
end)

PlayersDataStore:AutoSaveSet(Players)