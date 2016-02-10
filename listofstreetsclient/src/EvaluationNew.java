/*
 * ToDo 25.08.2013 not the real solution for mulitpolygons which are on a admin-boundary
 * 		select osm_id,tags->'name' as name, tags->'place' as place from ( select * from planet_polygon where ST_contains((select buffer(way,1000) from planet_polygon where osm_id = -453842), way) and (highway != '' OR place != '')) as foo where ST_Crosses((select way from planet_polygon where osm_id = -453842), way);
 * 		second query doesn't work well, should give one hit
 * 
 * ToDo 22.08.2013 modify usage of keyvariation_namelist. Up to now, only the most-last entry will be used.
 * 					the better way is to use all found name-variations. But in this case, a role have to be
 * 					added and/or the entries in new_street must be in relation for an identical street
 * 					to prevent all other name-variants as osm-only street in gui
 * 
 * 	V2.0, 06.04.2013
 * 		polygon will be stored now in standard-srid from osm_mapnik (900913), instead of previously 4326 (WGS84)
 * 		to eliminate little transformation-errors
 * 
		26.02.2013
			* activated place=village from ignore to active place-Object, because often a small hamlet is wrong tagged as a village

		OFFEN 12.12.2012
		-	
		11.12.2012
			*	not only name, but also official_name, loc_name and alt_name will be used for identifiying 
				objects (at this time, works only with one found name-variation, "name" most important)
			*	polygon_table will be used now for searching for place-objects, so place=hamlet, drawn as closed way for example,
				will be found


		OFFEN 03.11.2012.
		-	beim holen objete auf planet_polygon (z.b. relation 2298141 für einen Platz) wird "way" gesetzt, daher dürfte später dieses Objekt nicht aus josm geholt werden könenn.
			PRÜFEN
		-	area:highway= bei planet_polygon berücksichtigen
		-	alt_name prüfen und ggfs. mit berücksichtigen

		INFO:
		* mehrfache Straßennamen siehe 
			-	Sachsen%2FLeipzig%2FTh%C3%BCmmlitzwalde
			-	Fehmarn
 
		2012-01-21, Dietmar Seifert
	MISSING: if wtfe.org gets not an object-result, the way/node is okay.
		in this case, actual there will be created a missing-record in odblstate


		2011-12-05, Dietmar Seifert
			* highway=motorway and highway=motorway_link de-activate

		2011-12-04, Diemtar Seifert

		2011-12-01, Dietmar Seifert
			* highway=motorway and highway=motorway_link new in evaluation. Up to now they were be ignored
			* highway=proposed new in evalutation
			* place=locality new in evaluation
		
		OFFEN am 2011-06-29
			- verschiedene Codierungen Umlaute von overview nach Detailseite und ebenso von Detailseite nach geocoding
			- bei Auswertung weiler/isolated... berücksichtigen, entweder immer oder explizit per Aufforderung
*/

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.*;
import java.util.logging.*;

import de.diesei.listofstreets.StreetCollection;
import de.diesei.listofstreets.Applicationconfiguration;
import de.diesei.listofstreets.StreetCollection.StreetNameIdenticalLevel;

import java.net.URLDecoder;


/**
 * Auswertung einer Gemeinde oder eines größeren Gebiets.
 * Im Details wird die Auswertung über Aufrufparameter gestartet
 * und kontrolliert
 * 
 * @author Dietmar Seifert
 * @version 1.0
 * 
 * FERTIG:
 * - like => =
 * - System.out.println => Logger
 * - Transaction
 * 
 * OFFEN:
 * - Queries auf inner Join umgestellt
 * - HashMap ggfs. auf TreeMap umstellen
 */

public class EvaluationNew {
	
	private StreetNameIdenticalLevel streetnameIdenticalLevel = StreetCollection.StreetNameIdenticalLevel.EXACTLY;
	private String country = "Bundesrepublik Deutschland";
	private String municipality = "";
	private String officialkeysId = "";
	private String polygonstate = "";
	private boolean streetrefMustBeUsedForIdenticaly = false;
	private String streetrefOSMKey = "";

	public static final Logger logger = Logger.getLogger(EvaluationNew.class.getName());
	
	public StreetNameIdenticalLevel getStreetnameIdenticalLevel() {
		return streetnameIdenticalLevel;
	}

	public String getCountry() {
		return country;
	}

	public String getMunicipality() {
		return municipality;
	}

	public String getOfficialkeysId() {
		return this.officialkeysId;
	}

	public String getPolygonstate() {
		return this.polygonstate;
	}

	public boolean isStreetrefMustBeUsedForIdenticaly() {
		return streetrefMustBeUsedForIdenticaly;
	}
	
	public String getStreetrefOSMKey() {
		return streetrefOSMKey;
	}
	
	public void setStreetnameIdenticalLevel(StreetNameIdenticalLevel roughly) {
		this.streetnameIdenticalLevel = roughly;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public void setMunicipality(String municipality) {
		this.municipality = municipality;
	}

	public void setOfficialkeysId(String officialkeysid) {
		this.officialkeysId = officialkeysid;
	}

	public void setPolgonstate(String polyonstate) {
		this.polygonstate = polyonstate;
	}

	public void setStreetrefMustBeUsedForIdenticaly(
			String streetrefOSMKey) {
		this.streetrefMustBeUsedForIdenticaly = true;
		this.streetrefOSMKey = streetrefOSMKey;
	}

	public void unsetStreetrefMustBeUsedForIdenticaly() {
		this.streetrefMustBeUsedForIdenticaly = false;
		this.streetrefOSMKey = "";
	}

	
	
	
	static Connection con_listofstreets = null;
	static Connection con_mapnik = null;
	static Applicationconfiguration configuration = new Applicationconfiguration();


	
	public static void main(String args[]) {


		Boolean evaluation_type_full = false;
		Integer evaluation_overview_id = -1;
		String evaluation_text = "";

		
		java.util.Date time_program_startedtime = new java.util.Date();
		DateFormat time_formatter_mesz = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss z");
		DateFormat time_formatter_iso8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");		// in iso8601 format, with timezone
		DateFormat time_formatter_dateonly = new SimpleDateFormat("yyyy-MM-dd");


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

		
		logger.log(Level.FINE, "Program: Started Time: "+time_formatter_mesz.format(time_program_startedtime));
		
		
		
		for(int lfdnr=0;lfdnr<args.length;lfdnr++) {
			logger.log(Level.FINE, "args["+lfdnr+"] ==="+args[lfdnr]+"===");
		}
		if((args.length >= 1) && (args[0].equals("-h"))) {
			System.out.println("-polygonstate missing|ok|old|blocked");
			System.out.println("-country country");
			System.out.println("-name xystadt");
			System.out.println("-gemeindeschluessel 4711234");
			System.out.println("-osmrelationid 4711");
			System.out.println("-municipalityhierarchy somestring");
			System.out.println("-evaluationtext somestring");
			System.out.println("-evaluationtype full (optional)");
			return;
		}

		try {
			logger.log(Level.FINEST, "ok, jetzt Class.forName Aufruf ...");
			Class.forName("org.postgresql.Driver");
			logger.log(Level.FINEST, "ok, nach Class.forName Aufruf!");
		}
		catch(ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		try {
				//Connection of own project-specific DB
			String url_listofstreets = configuration.db_application_url;
			con_listofstreets = DriverManager.getConnection(url_listofstreets, configuration.db_application_username, configuration.db_application_password);
	
			String url_mapnik = configuration.db_osm2pgsql_url;
			con_mapnik = DriverManager.getConnection(url_mapnik, configuration.db_osm2pgsql_username, configuration.db_osm2pgsql_password);
	
			String dateizeile = "";


			StreetCollection osmstreets = new StreetCollection();
			StreetCollection liststreets = new StreetCollection();
			StreetCollection evaluatedstreets = new StreetCollection();

			java.util.Date time_act_municipality_starttime = null;
			java.util.Date time_last_step = null;
			java.util.Date time_now = null;

			java.util.Date time_evalation = null;
			java.util.Date time_osmdb = null;



					// Main Query
				// get all municipalities
//TODO prepared statement
			String sqlbefehl_jobs = "SELECT m.id AS id, country_id, c.country AS countryname, name , osm_hierarchy, officialkeys_id, osm_relation_id,"
				+ " sourcelist_url, sourcelist_text, sourcelist_deliverydate, sourcelist_filedate, polygon, polygon_state,"
				+ " parameters->'streetref' as parameterstreetref"
				+ " FROM municipality AS m, country AS c";
			String sqlbefehl_jobs_whereclause = "";
				//FIX IMMER SETZEN  ... blocked ...


			String parameter_polygonstate = "";

			if(args.length >= 1) {
				int args_ok_count = 0;
				for(int argsi=0;argsi<args.length;argsi+=2) {
					logger.log(Level.INFO, " args pair analysing #: "+argsi+"  ==="+args[argsi]+"===");
					if(args.length > argsi+1)
						logger.log(Level.INFO, "  args #+1: "+(argsi+1)+"   ==="+args[argsi+1]+"===");
					logger.log(Level.INFO, "");
					if(args[argsi].equals("-osmrelationid")) {
						if( ! sqlbefehl_jobs_whereclause.equals(""))
							sqlbefehl_jobs_whereclause += "AND ";
						sqlbefehl_jobs_whereclause += "osm_relation_id = '"+args[argsi+1]+"'";
						args_ok_count += 2;
					}
					if(args[argsi].equals("-gemeindeschluessel")) {
						if( ! sqlbefehl_jobs_whereclause.equals(""))
							sqlbefehl_jobs_whereclause += "AND ";
						sqlbefehl_jobs_whereclause += "officialkeys_id = '"+args[argsi+1]+"'";
						args_ok_count += 2;
					}
					if(args[argsi].equals("-polygonstate")) {
						parameter_polygonstate = args[argsi+1];
						if( ! sqlbefehl_jobs_whereclause.equals(""))
							sqlbefehl_jobs_whereclause += "AND ";
						sqlbefehl_jobs_whereclause += "polygon_state = '"+args[argsi+1]+"'";
						args_ok_count += 2;
					}
					if(args[argsi].equals("-country")) {
						logger.log(Level.FINE, "\ncountry parameter encoded ==="+URLDecoder.decode(args[argsi+1], "UTF-8")+"===");
						if( ! sqlbefehl_jobs_whereclause.equals(""))
							sqlbefehl_jobs_whereclause += "AND ";
						sqlbefehl_jobs_whereclause += "country = '"
							+ URLDecoder.decode(args[argsi+1], "UTF-8") + "' ";
						args_ok_count += 2;
					}
					if(args[argsi].equals("-name")) {
						logger.log(Level.FINE, "\ncountry name encoded ==="+URLDecoder.decode(args[argsi+1], "UTF-8")+"===");
						if( ! sqlbefehl_jobs_whereclause.equals(""))
							sqlbefehl_jobs_whereclause += "AND ";
						sqlbefehl_jobs_whereclause += "name = '"
							+ URLDecoder.decode(args[argsi+1], "UTF-8") + "' ";
						args_ok_count += 2;
					}
					if(args[argsi].equals("-municipalityhierarchy")) {
						if( ! sqlbefehl_jobs_whereclause.equals(""))
							sqlbefehl_jobs_whereclause += "AND ";
						sqlbefehl_jobs_whereclause += "osm_hierarchy like '%"+args[argsi+1]+"%'";
						args_ok_count += 2;
					}
					if(args[argsi].equals("-evaluationtext")) {
						evaluation_text = args[argsi+1];
						args_ok_count += 2;
					}
					if(args[argsi].equals("-evaluationtype")) {
						if(args[argsi+1].equals("full")) {
							evaluation_type_full = true;
							logger.log(Level.FINE, "ok, evaltype was correctly 'full'");
							System.out.println("ok, evaltype was correctly 'full'");
						} else {
							logger.log(Level.SEVERE, "ERROR: evaltype was wrong '"+args[argsi+1]+"'");
							System.out.println("ERROR: evaltype was wrong '"+args[argsi+1]+"'");
						}
						args_ok_count += 2;
					}
				}
				logger.log(Level.INFO, " Program-Call with arguments for main query filtering, here WHERE-Query part ==="+sqlbefehl_jobs_whereclause+"===");
				if(args_ok_count != args.length) {
					System.out.println("ERROR: not all programm parameters were valid, STOP");
					return;
				}
			} else {
				if( ! sqlbefehl_jobs_whereclause.equals(""))
					sqlbefehl_jobs_whereclause += "AND ";
				sqlbefehl_jobs_whereclause += "polygon_state <> 'blocked'";
			}
			if( ! sqlbefehl_jobs_whereclause.equals(""))
				sqlbefehl_jobs_whereclause += "AND ";
			sqlbefehl_jobs_whereclause += "m.country_id = c.id";

			if( ! sqlbefehl_jobs_whereclause.equals("")) {
				sqlbefehl_jobs += " WHERE " + sqlbefehl_jobs_whereclause;
			}

			sqlbefehl_jobs += " ORDER BY osm_hierarchy, name;";


			String osmosis_laststatefile = configuration.osmosis_laststatefile;

			Integer next_full_overview_id = -1;
			if(evaluation_type_full) {

				if(evaluation_text.equals("")) {
					java.util.Date temp_time = new java.util.Date();
					evaluation_text = time_formatter_dateonly.format(temp_time);
					logger.log(Level.FINE, "test zeit onlydate ==="+evaluation_text+"===");
				}

				String sql_max_full_overview_id = "SELECT MAX(evaluation_number) AS max_full_number FROM evaluation_overview where evaluation_type = 'full';";
				logger.log(Level.FINE, "hole SQL-Statement sql_max_full_overview_id ==="+sql_max_full_overview_id+"===");
				Statement stmt_max_full_overview_id = con_listofstreets.createStatement();
				ResultSet rs_max_full_overview_id = stmt_max_full_overview_id.executeQuery( sql_max_full_overview_id );
				if(rs_max_full_overview_id.next()) {
					next_full_overview_id = 1 + rs_max_full_overview_id.getInt("max_full_number");
					logger.log(Level.FINE, "got already used max number for full evaluation type ==="+next_full_overview_id+"===");
				}

				String insertbefehl_evalationoverview = "INSERT INTO evaluation_overview (evaluation_first_id, evaluation_last_id, description, evaluation_type, evaluation_number) ";
				insertbefehl_evalationoverview += "VALUES (-1, -1, '" + evaluation_text +"', 'full', " + next_full_overview_id + ");";
				logger.log(Level.FINE, "insertbefehl_evalationoverview ==="+insertbefehl_evalationoverview+"===");
				Statement stmt_insertevalationoverview = con_listofstreets.createStatement();
				String[] dbautogenkeys = { "id" };
				try {
					stmt_insertevalationoverview.executeUpdate( insertbefehl_evalationoverview, dbautogenkeys );

					ResultSet rs_getautogenkeys = stmt_insertevalationoverview.getGeneratedKeys();
				    while (rs_getautogenkeys.next()) {
				    	logger.log(Level.FINE, "Key returned from getGeneratedKeys():"
				            + rs_getautogenkeys.getInt(1));
				        evaluation_overview_id = rs_getautogenkeys.getInt("id");
				        logger.log(Level.FINE, "got new id from table evalation_overview, id ==="+evaluation_overview_id+"===");
				    } 
				    rs_getautogenkeys.close();

				}
				catch( SQLException e) {
					logger.log(Level.INFO, "ERROR: during insert in table evaluation_overview, insert code was ==="+insertbefehl_evalationoverview+"===");
					logger.log(Level.INFO, e.toString());
					System.out.println("ERROR: during insert in table evaluation_overview, insert code was ==="+insertbefehl_evalationoverview+"===");
					System.out.println(e.toString());
				}
			}


			java.util.Date time_detail_start = null;
			java.util.Date time_detail_end  = null;

			logger.log(Level.FINE, "hole SQL-Statement Job-Suche ==="+sqlbefehl_jobs+"===");
			Statement stmt_jobs = con_listofstreets.createStatement();
			ResultSet rs_jobs = stmt_jobs.executeQuery( sqlbefehl_jobs );
			String akt_municipality_id = "";
			String akt_jobname = "";
			String akt_gebietsgeometrie = "";
			String akt_parameterstreetref = "";
			Integer municipality_count = 0;
			while(rs_jobs.next()) {

				time_evalation = new java.util.Date();

				try {
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
					return;
				}
				
				time_act_municipality_starttime = new java.util.Date();
				time_last_step = time_act_municipality_starttime;

				municipality_count++;

				

				akt_municipality_id = rs_jobs.getString("id");
				akt_gebietsgeometrie = rs_jobs.getString("polygon");
				akt_parameterstreetref = rs_jobs.getString("parameterstreetref");

				String sqlbefehl_objekte = "";
				Statement stmt_objekte;
				ResultSet rs_objekte;

					// Array of all name-variants, which will be requested by sql-statement; most important at END
				String[] keyvariation_namelist = {"old_name", "loc_name", "alt_name", "official_name", "name"};

				if(rs_jobs.getString("polygon_state") != null) {
					if(		( !rs_jobs.getString("polygon_state").equals("ok"))
						&&	( !rs_jobs.getString("polygon_state").equals("new"))
						&&	( !rs_jobs.getString("polygon_state").equals("update"))
						&&	( !rs_jobs.getString("polygon_state").equals("old"))
						&&	( ! parameter_polygonstate.equals("")
						&&	( ! parameter_polygonstate.equals(rs_jobs.getString("polygon_state").equals("old"))))) {
						logger.log(Level.FINE, "municipality ==="+akt_jobname+"=== will not be evaluated, because polygon_state has wrong value ==="+rs_jobs.getString("polygon_state")+"===");
						continue;
					}

					EvaluationNew evaluation = new EvaluationNew();
					StreetlistReader listreader = new StreetlistReader();
					OsmDataReader osmreader = new OsmDataReader();
					
					osmstreets.clear();
					liststreets.clear();
					evaluatedstreets.clear();
						
					if(rs_jobs.getString("countryname").equals("Brasil"))
						evaluation.setStreetnameIdenticalLevel(StreetCollection.StreetNameIdenticalLevel.ROUGHLY);
					else
						evaluation.setStreetnameIdenticalLevel(StreetCollection.StreetNameIdenticalLevel.EXACTLY);
					evaluation.setCountry(rs_jobs.getString("countryname"));
					evaluation.setMunicipality(rs_jobs.getString("name"));
					evaluation.setOfficialkeysId(rs_jobs.getString("officialkeys_id"));
					evaluation.setPolgonstate(rs_jobs.getString("polygon_state"));
					if(		(rs_jobs.getString("parameterstreetref") != null) 
						&& 	(! rs_jobs.getString("parameterstreetref").equals("")))
						evaluation.setStreetrefMustBeUsedForIdenticaly(rs_jobs.getString("parameterstreetref"));
					else
						evaluation.unsetStreetrefMustBeUsedForIdenticaly();
					evaluation.setPolgonstate(rs_jobs.getString("polygon_state"));
						
					osmstreets = 
					
						
						// ------------------------------------------------------------------------------
							// 1. Action - get all street from osm table   PLANET_LINE   within boundary-polygon
										// DISTINCT ON (tags->'name')

						sqlbefehl_objekte = "SELECT";
						for(Integer nameindex = 0; nameindex < keyvariation_namelist.length; nameindex++) {
							String act_name = keyvariation_namelist[nameindex];
							sqlbefehl_objekte += " tags->'"+act_name+"' AS "+act_name+",";
						}
						sqlbefehl_objekte += " osm_id AS id, highway AS highwaytype, ST_AsText(ST_Transform(ST_Centroid(way),4326)) AS linecenterpoint";
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

						logger.log(Level.FINEST, "sqlbefehl_objekte (osm ways) ==="+sqlbefehl_objekte+"===");
						stmt_objekte = con_mapnik.createStatement();

						try {
							java.util.Date local_query_start = new java.util.Date();

							rs_objekte = stmt_objekte.executeQuery( sqlbefehl_objekte );

							java.util.Date local_query_end = new java.util.Date();
							logger.log(Level.FINEST, "TIME single-step quer osm-ways in ms. "+(local_query_end.getTime()-local_query_start.getTime()));

						}
						catch( SQLException sqlerror) {
							logger.log(Level.INFO, "ERROR: SQL-Exception during 1. Action - get all street from osm, sql-statement was ==="+sqlbefehl_objekte+"=== municipality   id==="+rs_jobs.getString("id")+"===  name ==="+rs_jobs.getString("name")+"==="); 
							logger.log(Level.INFO, sqlerror.toString());
							System.out.println("ERROR: SQL-Exception during 1. Action - get all street from osm, sql-statement was ==="+sqlbefehl_objekte+"=== municipality   id==="+rs_jobs.getString("id")+"===  name ==="+rs_jobs.getString("name")+"===");
							System.out.println(sqlerror.toString());
							continue;
						}
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
										temp_aktstrasse, rs_objekte.getString("streetref"), temp_linecenterpoint, temp_point_source);
								new_street = new_street.aktualisieren(streetobjects,streetobjects_anzahl, "osm", "way", local_osm_validkeyvalue, rs_objekte.getLong("id"), 
													temp_aktstrasse, rs_objekte.getString("streetref"), temp_linecenterpoint, temp_point_source);
								if(new_street.aktion.equals("fehlt")) {
									new_street = new_street.ergaenzen(streetobjects,streetobjects_anzahl, "osm", "way", local_osm_validkeyvalue, rs_objekte.getLong("id"), 
													temp_aktstrasse, rs_objekte.getString("streetref"), temp_linecenterpoint, temp_point_source);
									if(new_street.aktion.equals("neu")) {
										streetobjects[streetobjects_anzahl] = new_street;
										streetobjects_anzahl++;
									}
								}
							}
						}

						time_now = new java.util.Date();
						logger.log(Level.FINEST, "TIME all in ms. (get all streets in polygon) since actual municipality "+(time_now.getTime()-time_act_municipality_starttime.getTime())+"  since last step: "+(time_now.getTime()-time_last_step.getTime()));
						time_last_step = time_now;

						
							// ------------------------------------------------------------------------------
							// 1a. Action - get all street from osm table   PLANET_POLYGON   (so only closed ways) within boundary-polygon

						sqlbefehl_objekte = "SELECT ";
						for(Integer nameindex = 0; nameindex < keyvariation_namelist.length; nameindex++) {
							String act_name = keyvariation_namelist[nameindex];
							sqlbefehl_objekte += " tags->'"+act_name+"' AS "+act_name+",";
						}
						sqlbefehl_objekte += " osm_id AS id, highway as highwaytype, tags->'place' as place,";
						sqlbefehl_objekte += " ST_AsText(ST_Transform(ST_Centroid(way),4326)) as linecenterpoint";
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
						stmt_objekte = con_mapnik.createStatement();

						try {
							java.util.Date local_query_start = new java.util.Date();

							rs_objekte = stmt_objekte.executeQuery( sqlbefehl_objekte );

							java.util.Date local_query_end = new java.util.Date();
							logger.log(Level.FINEST, "TIME single-step closed osm-ways in ms. "+(local_query_end.getTime()-local_query_start.getTime()));
						}
						catch( SQLException sqlerror) {
							logger.log(Level.INFO, "ERROR: SQL-Exception during 1a. Action - get all closed way streets from osm, sql-statement was ==="+sqlbefehl_objekte+"=== municipality   id==="+rs_jobs.getString("id")+"===  name ==="+rs_jobs.getString("name")+"==="); 
							logger.log(Level.INFO, sqlerror.toString());
							System.out.println("ERROR: SQL-Exception during 1a. Action - get all closed way streets from osm, sql-statement was ==="+sqlbefehl_objekte+"=== municipality   id==="+rs_jobs.getString("id")+"===  name ==="+rs_jobs.getString("name")+"===");
							System.out.println(sqlerror.toString());
							continue;
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
								StreetObject new_street =  new StreetObject();
								new_street = new_street.aktualisieren(streetobjects,streetobjects_anzahl, "osm", "way", local_osm_validkeyvalue, rs_objekte.getLong("id"), 
													temp_aktstrasse, rs_objekte.getString("streetref"), temp_linecenterpoint, temp_point_source);
								if(new_street.aktion.equals("fehlt")) {
									new_street = new_street.ergaenzen(streetobjects,streetobjects_anzahl, "osm", "way", local_osm_validkeyvalue, rs_objekte.getLong("id"), 
													temp_aktstrasse, rs_objekte.getString("streetref"), temp_linecenterpoint, temp_point_source);
									if(new_street.aktion.equals("neu")) {
										streetobjects[streetobjects_anzahl] = new_street;
										streetobjects_anzahl++;
									}
								}
							}
						}

						time_now = new java.util.Date();
						logger.log(Level.FINEST, "TIME all in ms. (get all closed osm streets in polygon) since actual municipality "+(time_now.getTime()-time_act_municipality_starttime.getTime())+"  since last step: "+(time_now.getTime()-time_last_step.getTime()));
						time_last_step = time_now;



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
						sqlbefehl_objekte += " osm_id as id, tags->'place' as place, ST_AsText(ST_Transform(way,4326)) as point";
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
						stmt_objekte = con_mapnik.createStatement();
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
								StreetObject new_street =  new StreetObject();
								new_street = new_street.aktualisieren(streetobjects,streetobjects_anzahl, "osm", "node", local_osm_validkeyvalue, rs_objekte.getLong("id"), 
													temp_aktstrasse, rs_objekte.getString("streetref"), temp_point, temp_point_source);
								if(new_street.aktion.equals("fehlt")) {
									new_street = new_street.ergaenzen(streetobjects,streetobjects_anzahl, "osm", "node", local_osm_validkeyvalue, rs_objekte.getLong("id"), 
													temp_aktstrasse, rs_objekte.getString("streetref"), temp_point, temp_point_source);
									if(new_street.aktion.equals("neu")) {
										streetobjects[streetobjects_anzahl] = new_street;
										streetobjects_anzahl++;
									}
								}
							}
						}

						time_now = new java.util.Date();
						logger.log(Level.FINEST, "TIME all in ms. (get place-objects from nodes) since actual municipality "+(time_now.getTime()-time_act_municipality_starttime.getTime())+"  since last step: "+(time_now.getTime()-time_last_step.getTime()));
						time_last_step = time_now;

							// ------------------------------------------------------------------------------
							// 3. Action - get all street from street list
							// eigentlich als extra Lauf überflüssig, weil weiter unten die nur-Soll-Liste eigenständig erstellt wird
						//produktiv bis 03.03.2013, jetzt werden die Spalten point_state und point_source nicht mehr gesetzt, weil Tabelle evalation_street die Daten haelt - sqlbefehl_objekte = "SELECT DISTINCT ON (name) name, id, ST_AsText(point) AS point, point_state, point_source ";
						sqlbefehl_objekte = "SELECT DISTINCT ON (name) name, id, ST_AsText(point) AS point, point_source, streetref";
						sqlbefehl_objekte += " FROM street";
						sqlbefehl_objekte += " WHERE municipality_id = "+akt_municipality_id;
						sqlbefehl_objekte += " ORDER BY name;";
						logger.log(Level.FINEST, "sqlbefehl_objekte ==="+sqlbefehl_objekte+"===");
						stmt_objekte = con_listofstreets.createStatement();
						rs_objekte = stmt_objekte.executeQuery( sqlbefehl_objekte );
						while( rs_objekte.next() ) {
							anzahl_datensaetze_sollliste++;
							String temp_aktstrasse = rs_objekte.getString("name");
							logger.log(Level.FINER, "Soll-Straße ==="+temp_aktstrasse +"===    point_source==="+rs_objekte.getString("point_source")+"===");
							try {
								String temp_linecenterpoint = "";
								String temp_point_source = "";
								StreetObject new_street =  new StreetObject();
								new_street = new_street.aktualisieren(streetobjects,streetobjects_anzahl, "street", "", "", rs_objekte.getLong("id"), 
													rs_objekte.getString("name"), rs_objekte.getString("streetref"), temp_linecenterpoint, temp_point_source);
								if(new_street.aktion.equals("fehlt")) {
									new_street = new_street.ergaenzen(streetobjects,streetobjects_anzahl, "street", "", "", rs_objekte.getLong("id"), 
													rs_objekte.getString("name"), rs_objekte.getString("streetref"), temp_linecenterpoint, temp_point_source);
									if(new_street.aktion.equals("neu")) {
										streetobjects[streetobjects_anzahl] = new_street;
										streetobjects_anzahl++;
									}
								}
							} catch (NullPointerException nullerror) {
								logger.log(Level.WARNING, "NullpointerException happened at municipality_id ==="+akt_municipality_id+"=== ...");
							}
						}

						time_now = new java.util.Date();
						logger.log(Level.FINEST, "TIME all in ms. (get all streets from list) since actual municipality "+(time_now.getTime()-time_act_municipality_starttime.getTime())+"  since last step: "+(time_now.getTime()-time_last_step.getTime()));
						time_last_step = time_now;
		

						logger.log(Level.FINEST, " sorting starts ...");
						java.util.Date time_sort_startedtime = new java.util.Date();
						StreetObject.sort(streetobjects, streetobjects_anzahl);
						java.util.Date time_sort_endedtime = new java.util.Date();
						logger.log(Level.FINEST, " sorting ended");
						logger.log(Level.FINEST, "sort: Duration in ms: "+(time_sort_endedtime.getTime()-time_sort_startedtime.getTime()));
	
						time_now = new java.util.Date();
						logger.log(Level.FINEST, "TIME all in ms. (sorting all streets) since actual municipality "+(time_now.getTime()-time_act_municipality_starttime.getTime())+"  since last step: "+(time_now.getTime()-time_last_step.getTime()));
						time_last_step = time_now;

						String insert_evaluationstreet = "INSERT INTO evaluation_street (evaluation_id,street_id,name,osm_id,osm_type,osm_keyvalue) ";
						insert_evaluationstreet += "VALUES (?, ?, ?, ?, ?, ?);";
						logger.log(Level.FINEST, "insert sql for evaluation_street row ==="+insert_evaluationstreet+"===");
						
						
							// to enable transaction mode, disable auto-transation mode for every db-action
						con_listofstreets.setAutoCommit(false);
						
						Integer strassen_soll_anzahl = 0;
						Integer strassen_ist_anzahl = 0;
						Integer strassen_osm_single = 0;
						Integer strassen_osm_missing_local = 0;
	
						time_now = new java.util.Date();
						java.util.Date time_startloop = new java.util.Date();
						java.util.Date time_last_loopstep = new java.util.Date();
						logger.log(Level.FINEST, "TIME all in ms. (loop objectstreet ) since loop-start "+(time_now.getTime()-time_startloop.getTime())+"  since last step: "+(time_now.getTime()-time_last_loopstep.getTime()));
						time_last_loopstep = time_now;
	
							// store first part of municipality result in evaluation table, without street-numbers (happens below aber loop over all streets)
							// here we need the row-id for storing all street detail in street-loop
						Integer evaluation_id = -1;
						String insertbefehl_evalation = "INSERT INTO evaluation (country_id, municipality_id, evaluation_overview_id, ";
						insertbefehl_evalation += "number_liststreets, number_osmstreets, number_osmsinglestreets,number_missingstreets,tstamp, osmdb_tstamp) ";
						insertbefehl_evalation += "VALUES ("+rs_jobs.getLong("country_id")+","+rs_jobs.getLong("id")+"," + evaluation_overview_id + ", 0, 0, 0, 0,";
						insertbefehl_evalation += "'" + time_formatter_iso8601.format(time_evalation) + "'" + ",";
						insertbefehl_evalation += "'" + time_formatter_iso8601.format(time_osmdb) + "'" + ");";
						logger.log(Level.FINEST, "insertbefehl_evalation ==="+insertbefehl_evalation+"===");
						Statement stmt_insertevalation = con_listofstreets.createStatement();
						String[] dbautogenkeys = { "id" };
						try {
							stmt_insertevalation.executeUpdate( insertbefehl_evalation, dbautogenkeys );

							ResultSet rs_getautogenkeys = stmt_insertevalation.getGeneratedKeys();
						    while (rs_getautogenkeys.next()) {
						    	logger.log(Level.FINEST, "Key returned from getGeneratedKeys():"
						            + rs_getautogenkeys.getInt(1));
						        evaluation_id = rs_getautogenkeys.getInt("id");
						    } 
						    rs_getautogenkeys.close();
						}
						catch( SQLException e) {
							logger.log(Level.INFO, "ERROR: during insert in table evaluation, insert code was ==="+insertbefehl_evalation+"==="); 
							logger.log(Level.INFO, e.toString()); 
							System.out.println("ERROR: during insert in table evaluation, insert code was ==="+insertbefehl_evalation+"===");
							System.out.println(e.toString());
						}
						
						
						
							// ======================================================================================
							//   all street collected in an array structure.
							//		Now, loop over all streets and build the html-file and store the results on every single street level into table evaluation_streets
							// ======================================================================================


						for( int streeti=0;streeti<streetobjects_anzahl;streeti++) {
	
							StreetObject activestreet = streetobjects[streeti];

							Integer local_street_id = -1;
							
							if( ! activestreet.street_id.equals("")) {
								if(activestreet.street_id.indexOf(",") != -1) {
									System.out.println("WARNING: more than one street row found, take only first one ==="
										+ activestreet.street_id.substring(0,activestreet.street_id.indexOf(","))+"=== from list ==="+activestreet.street_id+"===");
									local_street_id = Integer.parseInt(activestreet.street_id.substring(0,activestreet.street_id.indexOf(",")));
								} else {
									local_street_id = Integer.parseInt(activestreet.street_id);
								}
							}
							String local_street_name = activestreet.name;
							if(local_street_name.indexOf("'") != -1) {
								local_street_name = local_street_name.replace("'","''");
								logger.log(Level.FINER, "actual street name contains hochkomma ===" + activestreet.name + "===, was changed for insert into evaluation_street to ==="+local_street_name+"===");
							}

							time_detail_start  = new java.util.Date();
								// first, store actual street in evaluation_street table

							
							PreparedStatement insertevaluationstreetStmt = con_listofstreets.prepareStatement(insert_evaluationstreet);
							insertevaluationstreetStmt.setLong(1, evaluation_id);
							insertevaluationstreetStmt.setLong(2, local_street_id);
							insertevaluationstreetStmt.setString(3, local_street_name);
							insertevaluationstreetStmt.setString(4, activestreet.osm_id);
							insertevaluationstreetStmt.setString(5, activestreet.osm_type);
							insertevaluationstreetStmt.setString(6, activestreet.osm_objectkeyvalue);
							logger.log(Level.FINE,"parameter 1                   evaluation_id ===" + evaluation_id + "===");
							logger.log(Level.FINE,"parameter 2                 local_street_id ===" + local_street_id + "===");
							logger.log(Level.FINE,"parameter 3               local_street_name ===" + local_street_name + "===");
							logger.log(Level.FINE,"parameter 4             activestreet.osm_id ===" + activestreet.osm_id + "===");
							logger.log(Level.FINE,"parameter 5           activestreet.osm_type ===" + activestreet.osm_type + "===");
							logger.log(Level.FINE,"parameter 6 activestreet.osm_objectkeyvalue ===" + activestreet.osm_objectkeyvalue + "===");
							try {
								insertevaluationstreetStmt.execute();
							}
							catch( SQLException e) {
								logger.log(Level.INFO, "ERROR: during insert in table evaluation, insert code was ==="+insert_evaluationstreet+"===");
								logger.log(Level.INFO, e.toString());
								System.out.println("ERROR: during insert in table evaluation, insert code was ==="+insert_evaluationstreet+"===");
								System.out.println(e.toString());
							}
							time_detail_end  = new java.util.Date();
							logger.log(Level.FINEST, "TIME single step sql INSERT INTO evaluation_street in ms. "+(time_detail_end.getTime()-time_detail_start.getTime()));
							
							time_detail_start  = new java.util.Date();
							
							if(activestreet.osm_id.equals("")) {
								strassen_soll_anzahl++;
								anzahl_datensaetze_singlesoll++;
							} else if(( ! activestreet.osm_id.equals("")) && (activestreet.street_id.equals(""))) {
								if(	(activestreet.osm_objectkeyvalue != null) &&
									( ! activestreet.osm_objectkeyvalue.equals("")) &&
									(activestreet.osm_objectkeyvalue.indexOf("place=") == 0)) {		// place= nur-osm-Objekte nicht aufführen, weder in nur-osm-Zahl noch mit Namen
								} else {
									strassen_osm_single++;
									anzahl_datensaetze_singleosm++;
								}
							} else if(( ! activestreet.osm_id.equals("")) && ( ! activestreet.street_id.equals(""))) {
								strassen_soll_anzahl++;
								strassen_ist_anzahl++;
							}
	
							if(activestreet.osm_id.equals("")) {
								strassen_osm_missing_local++;
							} // end of at least one osm-id available, so got odbl-state of all  - if(activestreet.osm_id.equals("")) {
						} // end of loop over all streets of actual municipality - for( int streeti=0;streeti<StreetObject.anzahl;streeti++) {

						time_now = new java.util.Date();
						logger.log(Level.FINEST, "TIME all in ms. (BEHIND loop objectstreet ) since loop-start "+(time_now.getTime()-time_startloop.getTime())+"  since last step: "+(time_now.getTime()-time_last_loopstep.getTime()));
						time_last_loopstep = time_now;
	
						logger.log(Level.FINE, "jetzt die soll single zahlen: anzahl_datensaetze_singlesoll: "+anzahl_datensaetze_singlesoll+"   strassen_osm_missing_local: "+strassen_osm_missing_local);
						logger.log(Level.FINE, "und die ist-anzahl osm: "+strassen_ist_anzahl);
						anzahl_datensaetze_osmstreets = strassen_ist_anzahl;
	
						time_now = new java.util.Date();
						logger.log(Level.FINEST, "TIME all in ms. (creating detail html file, only streets part) since actual municipality "+(time_now.getTime()-time_act_municipality_starttime.getTime())+"  since last step: "+(time_now.getTime()-time_last_step.getTime()));
						time_last_step = time_now;
	
						String time_osmdb_string = "unbekannt";
						if( time_osmdb_string != null) {
							time_osmdb_string = time_formatter_mesz.format(time_osmdb);
						}

						
						logger.log(Level.FINE, "Anzahl Strassentreffer in OSM: "+anzahl_datensaetze_osmstreets+"===");
						logger.log(Level.FINE, "   Anzahl Placetreffer in OSM: "+anzahl_datensaetze_osmplaces+"===");
						logger.log(Level.FINE, "   Anzahl Strassen Soll-Liste: "+anzahl_datensaetze_sollliste+"===");
						logger.log(Level.FINE, "  Anzahl Strassen Soll Single: "+anzahl_datensaetze_singlesoll+"===");
						logger.log(Level.FINE, "   Anzahl Strammen OSM Single: "+anzahl_datensaetze_singleosm+"===");
	
						time_now = new java.util.Date();
						logger.log(Level.FINEST, "TIME all in ms. (rest of write detail html file) since actual municipality "+(time_now.getTime()-time_act_municipality_starttime.getTime())+"  since last step: "+(time_now.getTime()-time_last_step.getTime()));
						time_last_step = time_now;


						logger.log(Level.FINE, "evalution time in iso8601 format ==="+time_formatter_iso8601.format(time_evalation)+"===");
	
						String update_evaluation = "UPDATE evaluation set ";
						update_evaluation += "number_liststreets = " + anzahl_datensaetze_sollliste;
						update_evaluation += ",number_osmstreets = " + anzahl_datensaetze_osmstreets;
						update_evaluation += ",number_missingstreets = " + anzahl_datensaetze_singlesoll;
						update_evaluation += ",number_osmsinglestreets = " + anzahl_datensaetze_singleosm;
						update_evaluation += " WHERE id = " + evaluation_id + ";";
						logger.log(Level.FINE, "update_evaluation ==="+update_evaluation+"===");
						Statement stmt_updateevaluation = con_listofstreets.createStatement();
						try {
							stmt_updateevaluation.executeUpdate( update_evaluation );
						}
						catch( SQLException e) {
							logger.log(Level.INFO, "ERROR: during insert in table evaluation, insert code was ==="+update_evaluation+"===");
							logger.log(Level.FINE, e.toString());
							System.out.println("ERROR: during insert in table evaluation, insert code was ==="+update_evaluation+"===");
							System.out.println(e.toString());
						}

							// transaction commit
						con_listofstreets.commit();
							// re-activate standard auto-transation mode for every db-action
						con_listofstreets.setAutoCommit(true);
						
						time_now = new java.util.Date();
						logger.log(Level.FINEST, "TIME all in ms. (end-of-actual-municipality entry to overview file) since actual municipality "+(time_now.getTime()-time_act_municipality_starttime.getTime())+"  since last step: "+(time_now.getTime()-time_last_step.getTime()));
						time_last_step = time_now;
						// else (polygon_state not ok and not old
				} // end of loop if polygon is in useable (polygon_state ok or old)

				logger.log(Level.FINE, "========================================================================================");

			}	// end of loop over all municiapalities

			java.util.Date time_program_endedtime = new java.util.Date();
			logger.log(Level.INFO, "Program: Ended Time: "+time_formatter_mesz.format(time_program_endedtime));
			logger.log(Level.FINE, "Program: Duration in s: "+(time_program_endedtime.getTime()-time_program_startedtime.getTime())/1000);
		}
		catch( SQLException e) {
			logger.log(Level.INFO, e.toString());
			System.out.println(e.toString());
			try {
				con_listofstreets.rollback();
				con_listofstreets.close();
			} catch( SQLException innere) {
				logger.log(Level.INFO, "inner sql-exception (tried to rollback transaction or to close connection ...");
				logger.log(Level.INFO, innere.toString());
				System.out.println("inner sql-exception (tried to rollback transaction or to close connection ...");
				System.out.println(innere.toString());
			}
			return;
		} catch (UnsupportedEncodingException e) {
			logger.log(Level.INFO, e.toString());
			System.out.println(e.toString());
		}
	}

}
