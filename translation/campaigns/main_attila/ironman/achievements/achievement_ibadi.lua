-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
--
-- 	MEDIEVAL KINGDOMS 1212 - IRONMAN: ACHIEVEMENT - IBADI
-- 	By: DETrooper
--
-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------

local achievement = {};

achievement.name = "АйБаді";
achievement.description = "Досліджуйте всі технології, сповідуючи ібадитський іслам.";
achievement.manual = false; -- Is unlocked during achievement turn start check.
achievement.requiredreligions = {"mk_rel_ibadi_islam"}; -- The player must be one of these religions.
achievement.requiredtechnologies = "all";
achievement.unlocked = false;
achievement.unlocktime = "";

return achievement;
