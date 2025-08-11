-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
--
-- 	MEDIEVAL KINGDOMS 1212 - IRONMAN: ACHIEVEMENT - TORAH! TORAH! TORAH!
-- 	By: DETrooper
--
-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------

local achievement = {};

achievement.name = "Тора! Тора! Тора!";
achievement.description = "Граючи за фракцію іудеїв, розоріть або розграбуйте та окупуйте Рим.";
achievement.manual = false; -- Is unlocked during achievement turn start check.
achievement.requiredreligions = {"att_rel_judaism"}; -- The player must be one of these religions.
achievement.sacksettlements = {"att_reg_italia_roma"}; -- The player must sack these settlements at least once.
achievement.unlocked = false;
achievement.unlocktime = "";

return achievement;
