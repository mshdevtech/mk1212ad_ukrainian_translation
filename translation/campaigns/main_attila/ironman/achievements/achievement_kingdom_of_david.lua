-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
--
-- 	MEDIEVAL KINGDOMS 1212 - IRONMAN: ACHIEVEMENT - KINGDOM OF DAVID
-- 	By: DETrooper
--
-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------

local achievement = {};

achievement.name = "Царство Давида";
achievement.description = "Граючи за фракцію іудеїв, захопіть регіон Єрусалим.";
achievement.manual = false; -- Is unlocked during achievement turn start check.
achievement.requiredregions = {"att_reg_palaestinea_aelia_capitolina"}; -- Regions required for this achievement to unlock.
achievement.requiredreligions = {"att_rel_judaism"}; -- The player must be one of these religions.
achievement.unlocked = false;
achievement.unlocktime = "";

return achievement;
