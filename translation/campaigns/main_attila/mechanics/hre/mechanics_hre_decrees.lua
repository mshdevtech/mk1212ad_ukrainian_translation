--------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------
--
-- 	MEDIEVAL KINGDOMS 1212 - MECHANICS: HOLY ROMAN EMPIRE DECREES
-- 	By: DETrooper
--
--------------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------------
-- System for the HRE's emperor to pass decrees with various boons and drawbacks for themself and the empire at large.

local hre_decree_duration = 15;

mkHRE.active_decree = "nil";
mkHRE.active_decree_turns_left = 0;
mkHRE.decrees = {
	-- Reforms are unlocked in order from first to last.
	{
		["key"] = "hre_decree_imperial_levies",
		["name"] = "Призыв импер. новобранцев",
		["cost"] = 15,
		["emperor_effect_bundle_key"] = "mk_effect_bundle_hre_decree_imperial_levies",
		["emperor_effects"] = {"Стоимость содержания: +10% для сухопутных отрядов", "Пополнение: +5%", "Численность войск: +1"},
		["member_effect_bundle_key"] = "mk_effect_bundle_hre_member_imperial_levies",
		["member_effects"] = {"Пополнение: +5%", "Численность войск: +1"}
	},
	{
		["key"] = "hre_decree_patronize_universities",
		["name"] = "Покровительство университетам",
		["cost"] = 15,
		["emperor_effect_bundle_key"] = "mk_effect_bundle_hre_decree_patronize_universities",
		["emperor_effects"] = {"Скорость исследований: +20%", "Опыт новых агентов: +3 для священнослужителей", "Налоговая ставка: -10%"},
		["member_effect_bundle_key"] = "mk_effect_bundle_hre_member_patronize_universities",
		["member_effects"] = {"Скорость исследований: +20%", "Опыт новых агентов: +3 для священнослужителей"}
	},
	{
		["key"] = "hre_decree_expand_bureaucracy",
		["name"] = "Расширение чинов. аппарата",
		["cost"] = 15,
		["emperor_effect_bundle_key"] = "mk_effect_bundle_hre_decree_expand_bureaucracy",
		["emperor_effects"] = {"Верность: +2", "Коррупция: +5%", "Налоговая ставка: +15%"},
		["member_effect_bundle_key"] = "mk_effect_bundle_hre_member_expand_bureaucracy",
		["member_effects"] = {"Верность: +2", "Налоговая ставка: +15%"}
	},
	{
		["key"] = "hre_decree_promote_commerce",
		["name"] = "Развитие торговли",
		["cost"] = 15,
		["emperor_effect_bundle_key"] = "mk_effect_bundle_hre_decree_promote_commerce",
		["emperor_effects"] = {"Торговый доход: -5% от торговых соглашений", "Дип. отношения: +10 со всеми фракциями", "Богатство: +25% от торговых построек"},
		["member_effect_bundle_key"] = "mk_effect_bundle_hre_member_promote_commerce",
		["member_effects"] = {"Дип. отношения: +10 со всеми фракциями", "Богатство: +25% от торговых построек"}
	},
	{
		["key"] = "hre_decree_lessen_tax_burdens",
		["name"] = "Снижение налогового бремени",
		["cost"] = 15,
		["emperor_effect_bundle_key"] = "mk_effect_bundle_hre_decree_lessen_tax_burdens",
		["emperor_effects"] = {"Прирост числ. населения: +0.5% для горожан и крестьян", "Налоговая ставка: -15%", "Общественный порядок: +5"},
		["member_effect_bundle_key"] = "mk_effect_bundle_hre_member_lessen_tax_burdens",
		["member_effects"] = {"Прирост числ. населения: +0.5% для горожан и крестьян", "Общественный порядок: +5"}
	},
	{
		["key"] = "hre_decree_encourage_development",
		["name"] = "Стимулирование строительства",
		["cost"] = 15,
		["emperor_effect_bundle_key"] = "mk_effect_bundle_hre_decree_encourage_development",
		["emperor_effects"] = {"Санитария: +2", "Стоимость строительства: -25%", "Налоговая ставка: -15%"},
		["member_effect_bundle_key"] = "mk_effect_bundle_hre_member_encourage_development",
		["member_effects"] = {"Санитария: +2", "Стоимость строительства: -25%"}
	}
};

function mkHRE:Add_Decree_Listeners()
	cm:add_listener(
		"FactionTurnStart_HRE_Decrees",
		"FactionTurnStart",
		true,
		function(context) FactionTurnStart_HRE_Decrees(context) end,
		true
	);
end

function FactionTurnStart_HRE_Decrees(context)
	local faction = context:faction();
	local turn_number = cm:model():turn_number();

	if mkHRE:Get_Faction_State(faction:name()) == "emperor" then
		if mkHRE.active_decree_turns_left > 0 then
			mkHRE.active_decree_turns_left = mkHRE.active_decree_turns_left - 1;

			if mkHRE.active_decree_turns_left == 0 then
				mkHRE.active_decree = "nil";
			end
		end

		if not faction:is_human() then
			if mkHRE.active_decree == "nil" then 
				local random_decree = mkHRE.decrees[cm:random_number(#mkHRE.decrees)];

				if mkHRE.imperial_authority >= random_decree["cost"] then
					mkHRE:Activate_Decree(random_decree["key"]);
				end
			end
		end
	end
end

function mkHRE:Activate_Decree(decree_key)
	for i = 1, #mkHRE.decrees do
		if mkHRE.decrees[i]["key"] == decree_key then
			Apply_Decree_Effect_Bundle(mkHRE.decrees[i]["emperor_effect_bundle_key"], mkHRE.decrees[i]["member_effect_bundle_key"]);

			mkHRE.imperial_authority = mkHRE.imperial_authority - mkHRE.decrees[i]["cost"];
			mkHRE.active_decree = decree_key;
			mkHRE.active_decree_turns_left = hre_decree_duration;

			if HasValue(mkHRE.factions, cm:get_local_faction()) then
				cm:show_message_event(
					cm:get_local_faction(),
					"message_event_text_text_mk_event_hre_decree_title",
					"message_event_text_text_mk_event_hre_decree_primary_"..decree_key,
					"message_event_text_text_mk_event_hre_decree_secondary",
					true, 
					704
				);
			end

			break;
		end
	end

	-- Some decrees increase population growth, so re-compute region growth.
	Refresh_Region_Growths_Population(true);
end

function mkHRE:Deactivate_Decree(decree_key)
	for i = 1, #mkHRE.decrees do
		if mkHRE.decrees[i]["key"] == decree_key then
			if mkHRE.decrees[i]["emperor_effect_bundle_key"] ~= "none" then
				cm:remove_effect_bundle(mkHRE.decrees[i]["member_effect_bundle_key"], mkHRE.emperor_key);
			end

			if mkHRE.decrees[i]["member_effect_bundle_key"] ~= "none" then
				for j = 1, #mkHRE.factions do
					local faction_name = mkHRE.factions[j];

					cm:remove_effect_bundle(mkHRE.decrees[i]["member_effect_bundle_key"], faction_name);
				end
			end

			mkHRE.active_decree = "nil";
			mkHRE.active_decree_turns_left = 0;

			break;
		end
	end

	-- Some decrees increase population growth, so re-compute region growth.
	Refresh_Region_Growths_Population(true);
end

function Get_Decree_Property(decree_key, decree_property)
	for i = 1, #mkHRE.decrees do
		if mkHRE.decrees[i]["key"] == decree_key and mkHRE.decrees[i][decree_property]  then
			return mkHRE.decrees[i][decree_property];
		end
	end
end

function Apply_Decree_Effect_Bundle(emperor_effect_bundle_key, member_effect_bundle_key)
	for i = 1, #mkHRE.factions do
		local faction_name = mkHRE.factions[i];

		if mkHRE:Get_Faction_State(faction_name) == "emperor" then
			cm:apply_effect_bundle(emperor_effect_bundle_key, faction_name, hre_decree_duration);
		else
			cm:apply_effect_bundle(member_effect_bundle_key, faction_name, hre_decree_duration);
		end
	end
end

function Get_Decree_Tooltip(decree_key)
	local decreestring = "";

	for i = 1, #mkHRE.decrees do
		if mkHRE.decrees[i]["key"] == decree_key then
			local decree_name = mkHRE.decrees[i]["name"];
			local decree_cost = tostring(mkHRE.decrees[i]["cost"]);

			decreestring = decree_name.."\n----------------------------------------------\nРезультат для фракции императора:";

			for j = 1, #mkHRE.decrees[i]["emperor_effects"] do
				decreestring = decreestring.."\n"..mkHRE.decrees[i]["emperor_effects"][j];
			end

			decreestring = decreestring.."\n\nРезультат для государств-членов империи:";

			for j = 1, #mkHRE.decrees[i]["member_effects"] do
				decreestring = decreestring.."\n"..mkHRE.decrees[i]["member_effects"][j];
			end

			decreestring = decreestring.."\n\nЭтот указ затребует "..decree_cost.." ед. Имперской власти и будет длиться на протяжении "..tostring(hre_decree_duration).." х.";
		end
	end

	return decreestring;
end

--------------------------------------------------------------
----------------------- SAVING / LOADING ---------------------
--------------------------------------------------------------
cm:register_saving_game_callback(
	function(context)
		cm:save_value("mkHRE.active_decree", mkHRE.active_decree, context);
		cm:save_value("mkHRE.active_decree_turns_left", mkHRE.active_decree_turns_left, context);
	end
);

cm:register_loading_game_callback(
	function(context)
		mkHRE.active_decree = cm:load_value("mkHRE.active_decree", "nil", context);
		mkHRE.active_decree_turns_left = cm:load_value("mkHRE.active_decree_turns_left", 0, context);
	end
);