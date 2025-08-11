JERUSALEM_KEY = "mk_fact_jerusalem";
JERUSALEM_REGION_KEY = "att_reg_palaestinea_aelia_capitolina";
subSack = "Розорити||Ви не захопите поселення, але заберете з нього здобич і пошкодите будівлі. Усіх полонених буде вбито.||Суспільний порядок знизиться, ставлення колишніх власників поселення до вас сильно погіршиться.";
subLootOccupy = "Розграбувати та окупувати||В результаті розграбування ви отримаєте гроші, але споруди в цьому поселенні будуть пошкоджені. Усіх полонених перетворять на рабів.||Суспільний порядок істотно погіршиться. Дипломатичні відносини з колишнім власником цих земель сильно постраждають.";

POPE_LIST = {
	{id = "Інокентій III",	name = "names_name_2147380345", year = 1198, 	age = 37},
	{id = "Гонорій III",		name = "names_name_2147380346", year = 1216.5, 	age = 67},
	{id = "Григорій IX",		name = "names_name_2147380347", year = 1227, 	age = 69},
	{id = "Целестин IV",		name = "names_name_2147380353", year = 1241.5, 	age = 58},
	{id = "Інокентій IV",		name = "names_name_2147380363", year = 1243.5, 	age = 48},
	{id = "Олександр IV",		name = "names_name_2147380367", year = 1254.5, 	age = 55},
	{id = "Урбан IV",			name = "names_name_2147380369", year = 1261.5, 	age = 66},
	{id = "Климент IV",			name = "names_name_2147380378", year = 1265, 	age = 62},
	{id = "Григорій X",		name = "names_name_2147380386", year = 1271.5, 	age = 61},
	{id = "Інокентій V",		name = "names_name_2147380394", year = 1276, 	age = 52},
	{id = "Іоанн XXI",		name = "names_name_2147380401", year = 1276.5, 	age = 60},
	{id = "Миколай III",		name = "names_name_2147380406", year = 1277.5, 	age = 61},
	{id = "Мартин IV",		name = "names_name_2147380412", year = 1281, 	age = 71},
	{id = "Гонорій IV",		name = "names_name_2147380421", year = 1285, 	age = 75},
	{id = "Миколай IV",		name = "names_name_2147380426", year = 1288, 	age = 60},
	{id = "Целестин V",		name = "names_name_2147380428", year = 1294, 	age = 86},
	{id = "Боніфацій VIII",	name = "names_name_2147380432", year = 1294.5, 	age = 62},
	{id = "Бенедикт XI",		name = "names_name_2147380442", year = 1303.5, 	age = 63},
	{id = "Климент V",			name = "names_name_2147380449", year = 1305, 	age = 41},
	{id = "Іоанн XXII",		name = "names_name_2147380458", year = 1316.5, 	age = 70},
	{id = "Бенедикт XII",		name = "names_name_2147380468", year = 1334.5, 	age = 52},
	{id = "Климент VI",			name = "names_name_2147380477", year = 1342, 	age = 51},
	{id = "Інокентій VI",		name = "names_name_2147380479", year = 1352.5, 	age = 70},
	{id = "Урбан V",			name = "names_name_2147380481", year = 1362.5, 	age = 53},
	{id = "Григорій XI",		name = "names_name_2147380486", year = 1370.5, 	age = 41},
	{id = "Урбан VI",			name = "names_name_2147380494", year = 1378, 	age = 60},
	{id = "Боніфацій IX",		name = "names_name_2147380499", year = 1389.5, 	age = 34},
	{id = "Інокентій VII",	name = "names_name_2147380503", year = 1404.5, 	age = 67},
	{id = "Григорій XII",		name = "names_name_2147380504", year = 1406.5, 	age = 82},
	{id = "Мартин V",			name = "names_name_2147380512", year = 1417.5, 	age = 48},
	{id = "Євгеній IV",		name = "names_name_2147380513", year = 1431, 	age = 47},
	{id = "Миколай V",			name = "names_name_2147380516", year = 1447, 	age = 49},
	{id = "Калікст III",		name = "names_name_2147380517", year = 1455, 	age = 76},
	{id = "Пій II",			name = "names_name_2147380527", year = 1458.5, 	age = 52},
	{id = "Павло II",			name = "names_name_2147380533", year = 1464.5, 	age = 47},
	{id = "Сікст IV",			name = "names_name_2147380540", year = 1471.5, 	age = 57},
	{id = "Інокентій VIII",	name = "names_name_2147380542", year = 1484.5, 	age = 51},
	{id = "Олександр VI",		name = "names_name_2147380552", year = 1492.5, 	age = 61}
};

GENERIC_POPE_NAMES = {
	"names_name_2147353214",
	"names_name_2147373587",
	"names_name_2147365942",
	"names_name_2147358797",
	"names_name_2147352530",
	"names_name_2147353466",
	"names_name_2147366881",
	"names_name_2147358744",
	"names_name_2147353621"
};

SCRIPTED_CRUSADES_LIST = {
	-- Scripted target region, optional message turn, begin turn, crusader mission key, defender mission key, scripted participants.
	["5"] = {"att_reg_aegyptus_oxyrhynchus", 7, 11, "mk_mission_crusades_take_cairo_fifth_crusade", "mk_mission_crusades_defense_cairo", {"mk_fact_france", "mk_fact_hre", "mk_fact_hungary", "mk_fact_jerusalem"}}
}

CRUSADE_TARGET_RELIGIONS = {
	"mk_rel_chr_cathar",
	"mk_rel_ibadi_islam",
	"att_rel_pag_slavic",
	"mk_rel_shia_islam",
	"att_rel_other",
	"att_rel_semitic_paganism"
}

CRUSADE_ANCILLARIES = {
	"mk_companion_all_general_crusades_knight_hospitaller",
	"mk_companion_all_general_crusades_knight_templar"
}

CRUSADE_RECRUITABLE_UNITS = {
	["early"] = {
		"mk_mio_t1_hospitaller_sergeant_spearmen",
		"mk_mio_t1_templar_sergeant_spearmen",
		"mk_mio_t1_hospitaller_knights_dismounted",
		"mk_mio_t1_templar_knights_dismounted",
		"mk_mio_t1_hospitaller_crossbowmen_sergeants",
		"mk_mio_t1_templar_crossbowmen_sergeants",
		"mk_mio_t1_hospitaller_mounted_sergeants",
		"mk_mio_t1_hospitaller_knights",
		"mk_mio_t1_templar_mounted_sergeants",
		"mk_mio_t1_templar_knights",
		"mk_mio_t1_teu_mounted_sergeants",
		"mk_mio_t1_teu_sariantbruder",
		"mk_mio_t1_teu_ritterbruder",
	}
}

CRUSADE_RECRUITABLE_UNITS_CAPS = {
	["early"] = {
		["mk_mio_t1_hospitaller_sergeant_spearmen"] = 2,
		["mk_mio_t1_templar_sergeant_spearmen"] = 2,
		["mk_mio_t1_hospitaller_knights_dismounted"] = 1,
		["mk_mio_t1_templar_knights_dismounted"] = 1,
		["mk_mio_t1_hospitaller_crossbowmen_sergeants"] = 2,
		["mk_mio_t1_templar_crossbowmen_sergeants"] = 2,
		["mk_mio_t1_hospitaller_mounted_sergeants"] = 1,
		["mk_mio_t1_hospitaller_knights"] = 1,
		["mk_mio_t1_templar_mounted_sergeants"] = 1,
		["mk_mio_t1_templar_knights"] = 1,
		["mk_mio_t1_teu_mounted_sergeants"] = 1,
		["mk_mio_t1_teu_sariantbruder"] = 1,
		["mk_mio_t1_teu_ritterbruder"] = 1
	}
}

CRUSADE_REGIONS = {
	-- Core European territories recieve priority over faraway lands.
	["att_reg_italia_roma"] = 10000, -- Rome
	["att_reg_aquitania_elusa"] = 2500, -- Toulouse
	["att_reg_britannia_superior_londinium"] = 2500, -- London
	["att_reg_germania_uburzis"] = 2500, -- Frankfurt
	["att_reg_carthaginensis_toletum"] = 2500, -- Toledo
	["att_reg_lugdunensis_turonum"] = 2500, -- Paris
	["att_reg_tarraconensis_tarraco"] = 2500, -- Barcelona
	-- Orthodox territories to be reconquered if lost to non-Christians.
	["att_reg_thracia_constantinopolis"] = 2000, -- Constantinople
	["att_reg_macedonia_thessalonica"] = 1500, -- Thessalonica
	-- Important Middle East territories.
	["att_reg_palaestinea_aelia_capitolina"] = 1000, -- Jerusalem
	["att_reg_aegyptus_oxyrhynchus"] = 500, -- Cairo
	["att_reg_baetica_corduba"] = 500, -- Cordoba
	["att_reg_syria_antiochia"] = 500, -- Antioch
	["att_reg_africa_carthago"] = 250, -- Tunis
	["att_reg_aegyptus_alexandria"] = 250 -- Alexandria
}

CRUSADE_REGIONS_IN_MIDDLE_EAST = {
	"att_reg_aegyptus_alexandria",
	"att_reg_aegyptus_oxyrhynchus",
	"att_reg_palaestinea_aelia_capitolina",
	"att_reg_syria_antiochia"
}

CRUSADE_DEFENSE_UNIT_LIST = 	"mk_ayy_t1_jund_spearmen,mk_ayy_t1_jund_spearmen,mk_ayy_t1_jund_spearmen,mk_ayy_t1_jund_spearmen,".. -- Spears
				"mk_ayy_t1_jund_swordsmen,mk_ayy_t1_jund_swordsmen,mk_ayy_t1_jund_swordsmen,mk_ayy_t1_jund_swordsmen,".. -- Swords
				"mk_ayy_t1_crossbowmen,mk_ayy_t1_crossbowmen,mk_ayy_t1_crossbowmen"; -- Ranged
