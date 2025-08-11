-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
--
-- 	MEDIEVAL KINGDOMS 1212 - IRONMAN: ACHIEVEMENT - THERE CAN ONLY BE ONE
-- 	By: DETrooper
--
-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------

local achievement = {};

achievement.name = "Залишитися повинен тільки один";
achievement.description = "Граючи за Візантійську імперію, Латинську імперію або Св. Римську імперію, досягніть того, щоб усі інші претенденти на титул римського імператора були знищені.";
achievement.manual = false; -- Is unlocked during achievement turn start check.
achievement.requireddeadfactions = {"mk_fact_epirus", "mk_fact_hre", "mk_fact_latinempire", "mk_fact_nicaea", "mk_fact_trebizond"}; -- These factions need to be destroyed (not counting if player is one of them).
achievement.requiredfactions = {"mk_fact_epirus", "mk_fact_hre", "mk_fact_latinempire", "mk_fact_nicaea", "mk_fact_trebizond"}; -- The player must be one of these factions.
achievement.unlocked = false;
achievement.unlocktime = "";

return achievement;
