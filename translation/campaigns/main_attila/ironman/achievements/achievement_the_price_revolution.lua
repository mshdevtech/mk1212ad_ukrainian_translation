-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
--
-- 	MEDIEVAL KINGDOMS 1212 - IRONMAN: ACHIEVEMENT - THE PRICE REVOLTUION
-- 	By: DETrooper
--
-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------

local achievement = {};

achievement.name = "Революція цін";
achievement.description = "Володійте 20 будівлями з виробництва срібла.";
achievement.manual = false; -- Is unlocked during achievement turn start check.
achievement.requiredbuildings = {"mk_bld_all_resources_silver_1", "mk_bld_all_resources_silver_2", "mk_bld_all_resources_silver_3", "mk_bld_all_resources_silver_4"};
achievement.requirednumbuildings = 20; -- # of any of the above buildings required.
achievement.unlocked = false;
achievement.unlocktime = "";

return achievement;
