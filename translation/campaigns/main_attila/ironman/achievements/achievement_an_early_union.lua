-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
--
-- 	MEDIEVAL KINGDOMS 1212 - IRONMAN: ACHIEVEMENT - AN EARLY UNION
-- 	By: DETrooper
--
-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------

local achievement = {};

achievement.name = "Рання унія";
achievement.description = "Граючи за Королівство Польське, зробіть Велике князівство Литовське своєю васальною державою.";
achievement.manual = false; -- Is unlocked during achievement turn start check.
achievement.requiredfactions = {"mk_fact_lesserpoland"}; -- The player must be one of these factions.
achievement.requiredvassals = {"mk_fact_lithuania"}; -- The player must have these factions as vassals.
achievement.unlocked = false;
achievement.unlocktime = "";

return achievement;
