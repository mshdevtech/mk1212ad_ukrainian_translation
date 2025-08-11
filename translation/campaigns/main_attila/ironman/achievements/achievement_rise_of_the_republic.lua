-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
--
-- 	MEDIEVAL KINGDOMS 1212 - IRONMAN: ACHIEVEMENT - RISE OF THE REPUBLIC
-- 	By: DETrooper
--
-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------

local achievement = {};

achievement.name = "Розквіт республіки";
achievement.description = "Граючи за торговельну фракцію (Генуезьку, Пізанську або Венеціанську республіку), досягніть того, щоб інші торговельні республіки були знищені.";
achievement.manual = false; -- Is unlocked during achievement turn start check.
achievement.requireddeadfactions = {"mk_fact_genoa", "mk_fact_pisa", "mk_fact_venice"}; -- These factions need to be destroyed (not counting if player is one of them).
achievement.requiredfactions = {"mk_fact_genoa", "mk_fact_pisa", "mk_fact_venice"}; -- The player must be one of these factions.
achievement.unlocked = false;
achievement.unlocktime = "";

return achievement;
