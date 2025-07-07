-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------
--
-- 	MEDIEVAL KINGDOMS 1212 - IRONMAN: ACHIEVEMENT - NORTH SEA EMPIRE
-- 	By: DETrooper
--
-----------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------

local achievement = {};

achievement.name = "Імперія Північного моря";
achievement.description = "Граючи за Королівство Данія, відновіть кордони володінь короля Канута Великого, завоювавши всю Скандинавію та Англію.";
achievement.manual = false; -- Is unlocked during achievement turn start check.
achievement.requiredfactions = {"mk_fact_denmark"}; -- The player must be one of these factions.
achievement.requiredregions = {  -- Regions required for this achievement to unlock.
	"att_reg_britannia_inferior_eboracum",
	"att_reg_britannia_inferior_lindum",
	"att_reg_britannia_superior_camulodunon",
	"att_reg_britannia_superior_corinium",
	"att_reg_britannia_superior_londinium",
	"att_reg_scandza_alabu",
	"att_reg_scandza_hafn",
	"att_reg_scandza_hrefnesholt"
};
achievement.unlocked = false;
achievement.unlocktime = "";

return achievement;
