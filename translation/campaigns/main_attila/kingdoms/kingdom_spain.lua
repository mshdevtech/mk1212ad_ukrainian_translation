-------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------
--
-- 	MEDIEVAL KINGDOMS 1212 - KINGDOM: SPAIN
-- 	By: DETrooper
--
-------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------

ARAGON_KEY = "mk_fact_aragon";
CASTILE_KEY = "mk_fact_castile";
NAVARRE_KEY = "mk_fact_navarre";
SPANISH_KINGDOM_MISSION_TURN = 4;

SPANISH_KINGDOM_FACTION = "NIL";
SPANISH_KINGDOM_REGIONS_OWNED = false;

function Add_Kingdom_Spain_Listeners()
	if SPANISH_KINGDOM_FACTION == "NIL" then
		cm:add_listener(
			"FactionTurnStart_Spain_Check",
			"FactionTurnStart",
			true,
			function(context) Spain_Check(context) end,
			true
		);
		cm:add_listener(
			"SettlementOccupied_Spanish_Regions_Check",
			"SettlementOccupied",
			true,
			function(context) Spanish_Regions_Check(context) end,
			true
		);
	end

	if cm:is_multiplayer() == false then
		Register_Decision(
			"form_kingdom_spain", 
			function() 	
				local conditionstring = "Умови:\n\n([[rgba:8:201:27:150]]Y[[/rgba]]) - Гра за Арагонську корону, Королівство Кастилія або Королівство Наварра.\n([[rgba:8:201:27:150]]Y[[/rgba:8:201:27:150]]) - Немає фракції Королівство Іспанія.\n";
				local faction_name = cm:get_local_faction();

				if mkHRE then
					if faction_name == mkHRE.emperor_key then
						conditionstring = conditionstring.."([[rgba:255:0:0:150]]X[[/rgba]]) - Ви не є імператором Св. Римської імперії.\n";
					else
						conditionstring = conditionstring.."([[rgba:8:201:27:150]]Y[[/rgba]]) - Ви не є імператором Св. Римської імперії.\n";
					end
				end
	
				for i = 1, #REGIONS_SPAIN_NO_PORTUGAL do
					local region = cm:model():world():region_manager():region_by_key(REGIONS_SPAIN_NO_PORTUGAL[i]);
					
					if region:owning_faction():name() == cm:get_local_faction() then
						conditionstring = conditionstring.."([[rgba:8:201:27:150]]Y[[/rgba]]) - Владение регионом "..REGIONS_NAMES_LOCALISATION[REGIONS_SPAIN_NO_PORTUGAL[i]]..".\n";
					else
						conditionstring = conditionstring.."([[rgba:255:0:0:150]]X[[/rgba]]) - Володіння регіоном "..REGIONS_NAMES_LOCALISATION[REGIONS_SPAIN_NO_PORTUGAL[i]]..".\n";
					end
				end
			
				conditionstring = conditionstring.."\nРезультат:\n\n- Перетворення у фракцію [[rgba:255:215:0:215]]Королівство Іспанія[[/rgba]].";
			
				return conditionstring;
			end, 
			REGIONS_SPAIN_NO_PORTUGAL, 
			{map_name = "kingdom_spain_map", x = "500", y = "500", map_pips = REGIONS_SPAIN_FACTION_PIPS_LOCATIONS},
			Spanish_Kingdom_Formed
		);

		if cm:is_new_game() then
			local faction_name = cm:get_local_faction();
			
			if faction_name == ARAGON_KEY or faction_name == CASTILE_KEY or faction_name == NAVARRE_KEY then
				Add_Decision("form_kingdom_spain", faction_name, false, true);
			end
		end
	end
end

function Spain_Check(context)
	local faction_name = context:faction():name();
	
	if faction_name == ARAGON_KEY or faction_name == CASTILE_KEY or faction_name == NAVARRE_KEY then
		Spanish_Regions_Check(context);
	end
end

function Spanish_Regions_Check(context)
	local faction_name = context:faction():name();
	local has_regions = Has_Required_Regions(faction_name, REGIONS_SPAIN_NO_PORTUGAL);
	SPANISH_KINGDOM_REGIONS_OWNED = has_regions;
		
	if has_regions == true then
		if cm:is_multiplayer() == true or context:faction():is_human() == false then
			Spanish_Kingdom_Formed(faction_name);
		elseif not mkHRE or faction_name ~= mkHRE.emperor_key then
			Enable_Decision("form_kingdom_spain");
		end
	end
end

function Spanish_Kingdom_Formed(faction_name)
	FACTIONS_DFN_LEVEL[faction_name] = 4;
	SPANISH_KINGDOM_FACTION = faction_name;
	Rename_Faction(faction_name, faction_name.."_lvl"..tostring(FACTIONS_DFN_LEVEL[faction_name]));

	if cm:is_multiplayer() == false then
		Remove_Decision("form_kingdom_spain");
		Add_Decision("found_an_empire", faction_name, false, false);
	end

	cm:show_message_event(
		faction_name, 
		"message_event_text_text_mk_event_spanish_kingdom_formed_title", 
		"message_event_text_text_mk_event_spanish_kingdom_formed_primary",
		"message_event_text_text_mk_event_spanish_kingdom_formed_secondary_"..faction_name,
		true, 
		722
	);

	cm:remove_listener("FactionTurnStart_Spain_Check");
	cm:remove_listener("SettlementOccupied_Spanish_Regions_Check");
end

--------------------------------------------------------------
----------------------- SAVING / LOADING ---------------------
--------------------------------------------------------------
cm:register_saving_game_callback(
	function(context)
		cm:save_value("SPANISH_KINGDOM_FACTION", SPANISH_KINGDOM_FACTION, context);
		cm:save_value("SPANISH_KINGDOM_REGIONS_OWNED", SPANISH_KINGDOM_REGIONS_OWNED, context);
	end
);

cm:register_loading_game_callback(
	function(context)
		SPANISH_KINGDOM_FACTION = cm:load_value("SPANISH_KINGDOM_FACTION", "NIL", context);
		SPANISH_KINGDOM_REGIONS_OWNED = cm:load_value("SPANISH_KINGDOM_REGIONS_OWNED", false, context);
	end
);
