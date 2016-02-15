

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import de.regioosm.listofstreetsclient.Applicationconfiguration;

public class OsmDataReader {
	Applicationconfiguration configuration = new Applicationconfiguration();
	private static Logger logger = (Logger) EvaluationNew.logger;


	static Connection con_mapnik = null;


	
	public StreetCollection ReadDataFromDB(EvaluationNew  evaluation) {
		StreetCollection streets = new StreetCollection();

		if(		(evaluation.getCountry().equals(""))
			||	(evaluation.getMunicipality().equals(""))) {
				return null;
			}		

		
		String akt_jobname = evaluation.getMunicipality();
		String akt_parameterstreetref = "";
		if(evaluation.isStreetrefMustBeUsedForIdenticaly()) {
			akt_parameterstreetref = evaluation.getStreetrefOSMKey();
		}
		
		streets.setEvaluationParameters(evaluation);
		
		try {

			Handler handler = new ConsoleHandler();
			handler.setLevel(configuration.logging_console_level);
			logger.addHandler( handler );
			FileHandler fhandler = new FileHandler(configuration.logging_filename);
			fhandler.setLevel(configuration.logging_file_level);
			logger.addHandler( fhandler );
			logger.setLevel(configuration.logging_console_level);
		} 
		catch (IOException e) {
			System.out.println("Fehler beim Logging-Handler erstellen, ...");
			System.out.println(e.toString());
		}

		String osmosis_laststatefile = configuration.osmosis_laststatefile;

		try {
			String url_mapnik = configuration.db_osm2pgsql_url;
			con_mapnik = DriverManager.getConnection(url_mapnik, configuration.db_osm2pgsql_username, configuration.db_osm2pgsql_password);
	
			String dateizeile = "";
	
	
			java.util.Date time_act_municipality_starttime = null;
			java.util.Date time_last_step = null;
			java.util.Date time_now = null;
	
			java.util.Date time_evalation = null;
			java.util.Date time_osmdb = null;
	
			DateFormat time_formatter_iso8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");		// in iso8601 format, with timezone
	
	
			java.util.Date time_detail_start = null;
			java.util.Date time_detail_end  = null;
	
			time_evalation = new java.util.Date();
	

				// read the filestamp of the update of the local osm-db. This timestamp will be stored together with the evaluation
			BufferedReader filereader = new BufferedReader(new InputStreamReader(new FileInputStream(osmosis_laststatefile), StandardCharsets.UTF_8));
					//#Fri Sep 21 07:39:59 CEST 2012
					//sequenceNumber=121
					//timestamp=2012-09-17T08\:00\:00Z
			while ((dateizeile = filereader.readLine()) != null) {
				if(dateizeile.indexOf("timestamp=") == 0) {
					String local_time = dateizeile.substring(dateizeile.indexOf("=")+1);
						// remove special charater \: to :
					local_time = local_time.replace("\\","");
						// change abbreviation Z to +00:00, otherwise parsing fails
					local_time = local_time.replace("Z","+0000");
					logger.log(Level.FINE, "local_time ==="+local_time+"===");
					java.util.Date temp_time = new java.util.Date();
					logger.log(Level.FINE, "test zeit iso8601 ==="+time_formatter_iso8601.format(temp_time)+"===");

					time_osmdb = time_formatter_iso8601.parse(local_time);
				}
			}
			filereader.close();
		}
		catch (Exception e) {
			logger.log(Level.INFO, "ERROR: failed to read osmosis last.state.txt file ==="+osmosis_laststatefile+"===");
			logger.log(Level.INFO, e.toString());
			System.out.println("ERROR: failed to read osmosis last.state.txt file ==="+osmosis_laststatefile+"===");
			System.out.println(e.toString());
			return streets;
		}
			


			
		Integer anzahl_datensaetze_osmstreets = 0;
		Integer anzahl_datensaetze_osmplaces = 0;
		Integer anzahl_datensaetze_sollliste = 0;
		Integer anzahl_datensaetze_singlesoll = 0;
		Integer anzahl_datensaetze_singleosm = 0;


		logger.log(Level.FINE, "========================================================================================");
		logger.log(Level.FINE, "   Job  =" + akt_jobname +"=   muni-id: "+evaluation.getmunicipalityDbId());
		logger.log(Level.FINE, "----------------------------------------------------------------------------------------");


		Long akt_municipality_id = evaluation.getmunicipalityDbId();
		String akt_gebietsgeometrie = evaluation.getMunitipalityGeometryBinaryString();
		String act_polygonstate = evaluation.getPolygonstate();

		String sqlbefehl_objekte = "";
		Statement stmt_objekte = null;
		ResultSet rs_objekte = null;

			// Array of all name-variants, which will be requested by sql-statement; most important at END
		String[] keyvariation_namelist = {"old_name", "loc_name", "alt_name", "official_name", "name"};


			// ------------------------------------------------------------------------------
			// 1. Action - get all street from osm table   PLANET_LINE   within boundary-polygon
						// DISTINCT ON (tags->'name')

		sqlbefehl_objekte = "SELECT";
		for(Integer nameindex = 0; nameindex < keyvariation_namelist.length; nameindex++) {
			String act_name = keyvariation_namelist[nameindex];
			sqlbefehl_objekte += " tags->'"+act_name+"' AS "+act_name+",";
		}
		sqlbefehl_objekte += " osm_id AS id, highway AS highwaytype,"
			+ " tags->'postal_code' AS postalcode,"
			+ " ST_AsText(ST_Transform(ST_Centroid(way),4326)) AS linecenterpoint";
		if(! akt_parameterstreetref.equals(""))
			sqlbefehl_objekte += ", tags->'" + akt_parameterstreetref + "' AS streetref";
		else
			sqlbefehl_objekte += ", null AS streetref";
		sqlbefehl_objekte += " FROM planet_line WHERE"
			+ " (ST_Covers('"+akt_gebietsgeometrie+"', way) OR"	// ST_Covers = complete inside
			+ " ST_Crosses('"+akt_gebietsgeometrie+"', way)) AND"  // ST_Crosses = some, but not all, common
			+ " tags ? 'highway'";
		for(Integer nameindex = 0; nameindex < keyvariation_namelist.length; nameindex++) {
			String act_name = keyvariation_namelist[nameindex];
			if(nameindex == 0)
				sqlbefehl_objekte += " AND (";
			sqlbefehl_objekte += " tags ? '"+act_name+"'";
			if(nameindex == (keyvariation_namelist.length -1))
				sqlbefehl_objekte += ")";
			else
				sqlbefehl_objekte += " OR";
		}
		sqlbefehl_objekte += " ORDER BY tags->'name';";


		try {
			logger.log(Level.FINEST, "sqlbefehl_objekte (osm ways) ==="+sqlbefehl_objekte+"===");
			stmt_objekte = con_mapnik.createStatement();

			java.util.Date local_query_start = new java.util.Date();

			rs_objekte = stmt_objekte.executeQuery( sqlbefehl_objekte );

			java.util.Date local_query_end = new java.util.Date();
			logger.log(Level.FINEST, "TIME single-step quer osm-ways in ms. "+(local_query_end.getTime()-local_query_start.getTime()));

			while( rs_objekte.next() ) {
				anzahl_datensaetze_osmstreets++;
					// get name of object
				String temp_aktstrasse = "";
				String act_name = "";
				for(Integer nameindex = 0; nameindex < keyvariation_namelist.length; nameindex++) {
					act_name = keyvariation_namelist[nameindex];
					if( rs_objekte.getString(act_name) != null) {
						temp_aktstrasse = rs_objekte.getString(act_name);
						if (!act_name.equals("name")) {
							logger.log(Level.FINER, "Ausgabe OSM-Straße Name-Variation (" + act_name + ") ==="+temp_aktstrasse +"===");
						}
					}
				}
				logger.log(Level.FINER, "OSM-Straße ==="+temp_aktstrasse +"===    osm-id ==="+rs_objekte.getString("id")+"===");
				String local_osm_validkeyvalue = "";
					// check for all possible values, optionally separated by ;
				String[] local_keyvalue = rs_objekte.getString("highwaytype").split(";");
				for(Integer local_keyvaluei=0;local_keyvaluei<local_keyvalue.length;local_keyvaluei++) {
					if( local_keyvalue[local_keyvaluei].equals("trunk") || 
						local_keyvalue[local_keyvaluei].equals("trunk_link") || 
						local_keyvalue[local_keyvaluei].equals("primary") || 
						local_keyvalue[local_keyvaluei].equals("primary_link") || 
						local_keyvalue[local_keyvaluei].equals("secondary") || 
						local_keyvalue[local_keyvaluei].equals("secondary_link") || 
						local_keyvalue[local_keyvaluei].equals("tertiary") || 
						local_keyvalue[local_keyvaluei].equals("tertiary_link") || 
						local_keyvalue[local_keyvaluei].equals("unclassified") || 
						local_keyvalue[local_keyvaluei].equals("living_street") || 
						local_keyvalue[local_keyvaluei].equals("pedestrian") || 
						local_keyvalue[local_keyvaluei].equals("construction") || 
						local_keyvalue[local_keyvaluei].equals("service") || 
						local_keyvalue[local_keyvaluei].equals("road") || 
						local_keyvalue[local_keyvaluei].equals("track") || 
						local_keyvalue[local_keyvaluei].equals("path") || 
						local_keyvalue[local_keyvaluei].equals("cycleway") || 
						local_keyvalue[local_keyvaluei].equals("steps") || 
						local_keyvalue[local_keyvaluei].equals("footway") || 
						local_keyvalue[local_keyvaluei].equals("residential") ||
						local_keyvalue[local_keyvaluei].equals("proposed")
						) {
							if( ! local_osm_validkeyvalue.equals(""))
								local_osm_validkeyvalue += ";" + local_keyvalue[local_keyvaluei];
							else
								local_osm_validkeyvalue = "highway=" + local_keyvalue[local_keyvaluei];
					} else if(	local_keyvalue[local_keyvaluei].equals("platform") || 
								local_keyvalue[local_keyvaluei].equals("bridleway") || 
								local_keyvalue[local_keyvaluei].equals("raceway") ||
								local_keyvalue[local_keyvaluei].equals("bus_stop") ||  
								local_keyvalue[local_keyvaluei].equals("rest_area")
							) {
						logger.log(Level.FINEST, " Info: ignore highway="+local_keyvalue[local_keyvaluei]+"  highway-name ==="+temp_aktstrasse+"===");
					} else {
						logger.log(Level.FINE, " 1. Action highway unexpected value, please check ==="+local_keyvalue[local_keyvaluei]+"===   street name ==="+temp_aktstrasse+"===  osm-id ==="+rs_objekte.getString("id")+"===");
					}
				}
	
				if( ! local_osm_validkeyvalue.equals("")) {
					String temp_linecenterpoint = rs_objekte.getString("linecenterpoint");
					String temp_point_source = "";
					if( ! temp_linecenterpoint.equals(""))
						temp_point_source = "OSM";
					Street new_street =  new Street("osm", "way", local_osm_validkeyvalue, rs_objekte.getLong("id"), 
							temp_aktstrasse, rs_objekte.getString("streetref"), rs_objekte.getString("postalcode"),
							temp_linecenterpoint, temp_point_source);
					if(streets.get(new_street) != null)
							streets.get(new_street).update(new_street);
					else
							streets.add(new_street);
				}
			}
	
	
				
				// ------------------------------------------------------------------------------
				// 1a. Action - get all street from osm table   PLANET_POLYGON   (so only closed ways) within boundary-polygon
	
			sqlbefehl_objekte = "SELECT ";
			for(Integer nameindex = 0; nameindex < keyvariation_namelist.length; nameindex++) {
				String act_name = keyvariation_namelist[nameindex];
				sqlbefehl_objekte += " tags->'"+act_name+"' AS "+act_name+",";
			}
			sqlbefehl_objekte += " osm_id AS id, highway as highwaytype, tags->'place' as place,"
				+ " tags->'postal_code' AS postalcode,"
				+ " ST_AsText(ST_Transform(ST_Centroid(way),4326)) as linecenterpoint";
			if(! akt_parameterstreetref.equals(""))
				sqlbefehl_objekte += ", tags->'" + akt_parameterstreetref + "' AS streetref";
			else
				sqlbefehl_objekte += ", null AS streetref";
			sqlbefehl_objekte += " FROM planet_polygon";
			sqlbefehl_objekte += " WHERE ST_Covers('"+akt_gebietsgeometrie+"', way) AND";		//ST_Centroid(way) seems to take too long
			sqlbefehl_objekte += " (highway != '' OR place != '')";
			for(Integer nameindex = 0; nameindex < keyvariation_namelist.length; nameindex++) {
				String act_name = keyvariation_namelist[nameindex];
				if(nameindex == 0)
					sqlbefehl_objekte += " AND (";
				sqlbefehl_objekte += " tags ? '"+act_name+"'";
				if(nameindex == (keyvariation_namelist.length -1))
					sqlbefehl_objekte += ")";
				else
					sqlbefehl_objekte += " OR";
			}
			sqlbefehl_objekte += " ORDER BY name;";
	
			logger.log(Level.FINEST, "sqlbefehl_objekte (osm closed ways) ==="+sqlbefehl_objekte+"===");
			//stmt_objekte = con_mapnik.createStatement();
	
			try {
				local_query_start = new java.util.Date();
	
				rs_objekte = stmt_objekte.executeQuery( sqlbefehl_objekte );
	
				local_query_end = new java.util.Date();
				logger.log(Level.FINEST, "TIME single-step closed osm-ways in ms. "+(local_query_end.getTime()-local_query_start.getTime()));
			}
			catch( SQLException sqlerror) {
				logger.log(Level.INFO, "ERROR: SQL-Exception during 1a. Action - get all closed way streets from osm, sql-statement was ==="+sqlbefehl_objekte+"=== municipality   id==="+evaluation.getmunicipalityDbId()+"===  name ==="+evaluation.getMunicipality()+"==="); 
				logger.log(Level.INFO, sqlerror.toString());
				System.out.println("ERROR: SQL-Exception during 1a. Action - get all closed way streets from osm, sql-statement was ==="+sqlbefehl_objekte+"=== municipality   id==="+evaluation.getmunicipalityDbId()+"===  name ==="+evaluation.getMunicipality()+"===");
				System.out.println(sqlerror.toString());
				try {
					if(rs_objekte != null)
						rs_objekte.close();
					if(stmt_objekte != null)
						stmt_objekte.close();
					if(con_mapnik != null)
						con_mapnik.close();
					return null;
				} 
				catch(Exception e) {
					logger.log(Level.SEVERE, "General Exception, Details: " + e.toString());
					System.out.println("General Exception, Details: " + e.toString());
				}
				return null;
			}

			while( rs_objekte.next() ) {
				String local_osm_validkeyvalue = "";
	
					// get name of object
				String temp_aktstrasse = "";
				String act_name = "";
				for(Integer nameindex = 0; nameindex < keyvariation_namelist.length; nameindex++) {
					act_name = keyvariation_namelist[nameindex];
					if( rs_objekte.getString(act_name) != null) {
						temp_aktstrasse = rs_objekte.getString(act_name);
						if (!act_name.equals("name")) {
							logger.log(Level.FINER, "Ausgabe OSM-Straße Name-Variation (" + act_name + ") ==="+temp_aktstrasse +"===");
						}
					}
				}
	
				if(rs_objekte.getString("highwaytype") != null) {
					anzahl_datensaetze_osmstreets++;
	
						// check for all possible values, optionally separated by ;
					String[] local_keyvalue = rs_objekte.getString("highwaytype").split(";");
					for(Integer local_keyvaluei=0;local_keyvaluei<local_keyvalue.length;local_keyvaluei++) {
						if( local_keyvalue[local_keyvaluei].equals("trunk") || 
							local_keyvalue[local_keyvaluei].equals("trunk_link") || 
							local_keyvalue[local_keyvaluei].equals("primary") || 
							local_keyvalue[local_keyvaluei].equals("primary_link") || 
							local_keyvalue[local_keyvaluei].equals("secondary") || 
							local_keyvalue[local_keyvaluei].equals("secondary_link") || 
							local_keyvalue[local_keyvaluei].equals("tertiary") || 
							local_keyvalue[local_keyvaluei].equals("tertiary_link") || 
							local_keyvalue[local_keyvaluei].equals("unclassified") || 
							local_keyvalue[local_keyvaluei].equals("living_street") || 
							local_keyvalue[local_keyvaluei].equals("pedestrian") || 
							local_keyvalue[local_keyvaluei].equals("construction") || 
							local_keyvalue[local_keyvaluei].equals("service") || 
							local_keyvalue[local_keyvaluei].equals("road") || 
							local_keyvalue[local_keyvaluei].equals("track") || 
							local_keyvalue[local_keyvaluei].equals("path") || 
							local_keyvalue[local_keyvaluei].equals("cycleway") || 
							local_keyvalue[local_keyvaluei].equals("steps") || 
							local_keyvalue[local_keyvaluei].equals("footway") || 
							local_keyvalue[local_keyvaluei].equals("residential") ||
							local_keyvalue[local_keyvaluei].equals("proposed")
							) {
								if( ! local_osm_validkeyvalue.equals(""))
									local_osm_validkeyvalue += ";" + local_keyvalue[local_keyvaluei];
								else
									local_osm_validkeyvalue = "highway=" + local_keyvalue[local_keyvaluei];
								//System.out.println(" 1. Action Treffer highway ==="+local_keyvalue[local_keyvaluei]+"===   name ==="+temp_aktstrasse+"===");
						} else if	(	local_keyvalue[local_keyvaluei].equals("platform") || 
										local_keyvalue[local_keyvaluei].equals("bridleway") || 
										local_keyvalue[local_keyvaluei].equals("raceway") || 
										local_keyvalue[local_keyvaluei].equals("bus_stop") ||  
										local_keyvalue[local_keyvaluei].equals("rest_area")
											) {
							logger.log(Level.FINEST, " Info: ignore highway="+local_keyvalue[local_keyvaluei]+"  highway-name ==="+temp_aktstrasse+"===");
						} else {
							logger.log(Level.FINE, " 1. Action highway unexpected value, please check ==="+local_keyvalue[local_keyvaluei]+"===   name ==="+temp_aktstrasse+"===");
						}
					}
				}  // end of type highway
				if(rs_objekte.getString("place") != null) {
					anzahl_datensaetze_osmplaces++;
					logger.log(Level.FINER, "OSM-Placename ==="+act_name+"===   place-value ==="+rs_objekte.getString("place")+"===   osm-id ==="+rs_objekte.getString("id")+"===");
						// check for all possible values, optionally separated by ;
					String[] local_keyvalue = rs_objekte.getString("place").split(";");
					for(Integer local_keyvaluei=0;local_keyvaluei<local_keyvalue.length;local_keyvaluei++) {
						if(	local_keyvalue[local_keyvaluei] == null) {
							logger.log(Level.WARNING, "Warning: ignorierung record with null- place-Tag");
							continue;
						}
	
						if(	local_keyvalue[local_keyvaluei].equals("hamlet") 
							||	local_keyvalue[local_keyvaluei].equals("village")					//moved from ignore to active at 2013-02-26
							|| local_keyvalue[local_keyvaluei].equals("isolated_dwelling")
							|| local_keyvalue[local_keyvaluei].equals("neighbourhood")				//moved from ignore to active at 2014-01-27 for Mannheim
							|| local_keyvalue[local_keyvaluei].equals("suburb")						//moved from ignore to actige at 2014-01-07
							|| local_keyvalue[local_keyvaluei].equals("locality")					//new evaluate at 2011-12-01
							|| local_keyvalue[local_keyvaluei].equals("farm") 						//new evaluate at 2014-02-15
							) {
								if( ! local_osm_validkeyvalue.equals(""))
									local_osm_validkeyvalue += ";" + local_keyvalue[local_keyvaluei];
								else
									local_osm_validkeyvalue = "place=" + local_keyvalue[local_keyvaluei];
						} else if(	local_keyvalue[local_keyvaluei].equals("state") ||
									local_keyvalue[local_keyvaluei].equals("region") ||
									local_keyvalue[local_keyvaluei].equals("county") || 
									local_keyvalue[local_keyvaluei].equals("municipality") || 
									local_keyvalue[local_keyvaluei].equals("town") || 
									local_keyvalue[local_keyvaluei].equals("city") ||  
									local_keyvalue[local_keyvaluei].equals("quarter") ||
									local_keyvalue[local_keyvaluei].equals("island") || 
									local_keyvalue[local_keyvaluei].equals("islet")
									) {
							logger.log(Level.FINEST, " Info: ignore place ="+local_keyvalue[local_keyvaluei]+"  place-name ==="+temp_aktstrasse+"===");
						} else {
							logger.log(Level.FINE, "Info: unexcpeted value for place ="+local_keyvalue[local_keyvaluei]+"  place-name ==="+temp_aktstrasse+"===");
						}
					}
				}	// end of type place	
	
				if( ! local_osm_validkeyvalue.equals("")) {
					logger.log(Level.FINEST, " found interesting place-object with name, osm-keyvalues ===" + local_osm_validkeyvalue + "===");
					String temp_linecenterpoint = rs_objekte.getString("linecenterpoint");
					String temp_point_source = "";
					if( ! temp_linecenterpoint.equals(""))
						temp_point_source = "OSM";
					Street new_street =  new Street("osm", "way", local_osm_validkeyvalue, rs_objekte.getLong("id"), 
							temp_aktstrasse, rs_objekte.getString("streetref"), rs_objekte.getString("postalcode"),
							temp_linecenterpoint, temp_point_source);
					if(streets.get(new_street) != null)
							streets.update(new_street);
					else
							streets.add(new_street);
				}
			}
	
	
	
	
				// ------------------------------------------------------------------------------
				// 2. Action - get place-objects with some values from osm table nodes
				//  active values should be     							hamlet, isolated_dwelling
				//  to ignore values should be    						..., 
				//  false set values, so should be active:  	locality
				//  unsure																		village  town
	
			sqlbefehl_objekte = "SELECT";
			for(Integer nameindex = 0; nameindex < keyvariation_namelist.length; nameindex++) {
				String act_name = keyvariation_namelist[nameindex];
				sqlbefehl_objekte += " tags->'"+act_name+"' AS "+act_name+",";
			}
			sqlbefehl_objekte += " osm_id as id, tags->'place' as place, tags->'postal_code' AS postalcode,"
				+ " ST_AsText(ST_Transform(way,4326)) as point";
			if(! akt_parameterstreetref.equals(""))
				sqlbefehl_objekte += ", tags->'" + akt_parameterstreetref + "' AS streetref";
			else
				sqlbefehl_objekte += ", null AS streetref";
			sqlbefehl_objekte += " FROM planet_point";
			sqlbefehl_objekte += " WHERE ST_Covers('"+akt_gebietsgeometrie+"',way) AND";
			sqlbefehl_objekte += " (tags ? 'place')";
			for(Integer nameindex = 0; nameindex < keyvariation_namelist.length; nameindex++) {
				String act_name = keyvariation_namelist[nameindex];
				if(nameindex == 0)
					sqlbefehl_objekte += " AND (";
				sqlbefehl_objekte += " tags ? '"+act_name+"'";
				if(nameindex == (keyvariation_namelist.length -1))
					sqlbefehl_objekte += ")";
				else
					sqlbefehl_objekte += " OR";
			}
			sqlbefehl_objekte += " ORDER BY name;";
	
			logger.log(Level.FINEST, "sqlbefehl_objekte (places from osm nodes) ==="+sqlbefehl_objekte+"===");
			//stmt_objekte = con_mapnik.createStatement();
			rs_objekte = stmt_objekte.executeQuery( sqlbefehl_objekte );
			while( rs_objekte.next() ) {
				anzahl_datensaetze_osmplaces++;
	
					// get name of object
				String temp_aktstrasse = "";
				String act_name = "";
				for(Integer nameindex = 0; nameindex < keyvariation_namelist.length; nameindex++) {
					act_name = keyvariation_namelist[nameindex];
					if( rs_objekte.getString(act_name) != null) {
						temp_aktstrasse = rs_objekte.getString(act_name);
						if (!act_name.equals("name")) {
							logger.log(Level.FINER, "Ausgabe OSM-Straße Name-Variation (" + act_name + ") ==="+temp_aktstrasse +"===");
						}
					}
				}
				logger.log(Level.FINER, "OSM-Placename ==="+temp_aktstrasse+"===   place-value ==="+rs_objekte.getString("place")+"===   osm-id ==="+rs_objekte.getString("id")+"===");
				String temp_point = rs_objekte.getString("point");
				String temp_point_source = "";
				String local_osm_validkeyvalue = "";
					// check for all possible values, optionally separated by ;
				String[] local_keyvalue = rs_objekte.getString("place").split(";");
				for(Integer local_keyvaluei=0;local_keyvaluei<local_keyvalue.length;local_keyvaluei++) {
					if(	local_keyvalue[local_keyvaluei] == null) {
						logger.log(Level.WARNING, "Warning: ignorierung record with null- place-Tag");
						continue;
					}
					
					if(	local_keyvalue[local_keyvaluei].equals("hamlet") 
							|| local_keyvalue[local_keyvaluei].equals("village")				//moved from ignore to active at 2013-02-26
							|| local_keyvalue[local_keyvaluei].equals("isolated_dwelling")
							|| local_keyvalue[local_keyvaluei].equals("neighbourhood")			//moved from ignore to active at 2014-01-27 for Mannheim
							|| local_keyvalue[local_keyvaluei].equals("suburb")					//moved from ignore to actige at 2014-01-07
							|| local_keyvalue[local_keyvaluei].equals("locality")				//new evaluate at 2011-12-01
							|| local_keyvalue[local_keyvaluei].equals("farm") 					//new evaluate at 2014-02-15
							) {
						if( ! local_osm_validkeyvalue.equals(""))
							local_osm_validkeyvalue += ";" + local_keyvalue[local_keyvaluei];
						else
							local_osm_validkeyvalue = "place=" + local_keyvalue[local_keyvaluei];
					} else if(	local_keyvalue[local_keyvaluei].equals("state") ||
								local_keyvalue[local_keyvaluei].equals("region") ||
								local_keyvalue[local_keyvaluei].equals("county") || 
								local_keyvalue[local_keyvaluei].equals("municipality") || 
								local_keyvalue[local_keyvaluei].equals("town") || 
								local_keyvalue[local_keyvaluei].equals("city") ||  
								local_keyvalue[local_keyvaluei].equals("quarter") ||
								local_keyvalue[local_keyvaluei].equals("island") || 
								local_keyvalue[local_keyvaluei].equals("islet")
							) {
						logger.log(Level.FINEST, " Info: ignore place ="+local_keyvalue[local_keyvaluei]+"  place-name ==="+temp_aktstrasse+"===");
					} else {
						logger.log(Level.WARNING, " Info: unexcpeted value for place ="+local_keyvalue[local_keyvaluei]+"  place-name ==="+temp_aktstrasse+"===");
					}
				}
	
				if( ! local_osm_validkeyvalue.equals("")) {
					logger.log(Level.FINEST, " found interesting place-object with name, osm-keyvalues ===" + local_osm_validkeyvalue + "===");
					if( ! temp_point.equals("")) {
						temp_point_source = "OSM";
					}
					Street new_street =  new Street("osm", "node", local_osm_validkeyvalue, rs_objekte.getLong("id"), 
							temp_aktstrasse, rs_objekte.getString("streetref"), rs_objekte.getString("postalcode"), 
							temp_point, temp_point_source);
					if(streets.get(new_street) != null)
							streets.update(new_street);
					else
							streets.add(new_street);
				}
			}
			rs_objekte.close();
			stmt_objekte.close();
			con_mapnik.close();
		}
		catch( SQLException sqle) {
			logger.log(Level.INFO, sqle.toString());
			System.out.println(sqle.toString());
			try {
				if(rs_objekte != null)
					rs_objekte.close();
				if(stmt_objekte != null)
					stmt_objekte.close();
				if(con_mapnik != null)
					con_mapnik.close();
				return null;
			} 
			catch(Exception e) {
				logger.log(Level.SEVERE, "General Exception, Details: " + e.toString());
				System.out.println("General Exception, Details: " + e.toString());
			}
		}
		return streets;
	}
}
