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
import java.util.Map;
import java.util.logging.*;

import de.regioosm.listofstreetsclient.Applicationconfiguration;

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
	
	private StreetNameIdenticalLevel streetnameIdenticalLevel = StreetNameIdenticalLevel.EXACTLY;
	private String country = "Bundesrepublik Deutschland";
	private Long countryDbId = -1L;
	private String municipality = "";
	private Long municipalityDbId = -1L;
	private String municipalityGeometryBinaryString = "";
	private String officialkeysId = "";
	private String polygonstate = "";
	private boolean streetrefMustBeUsedForIdenticaly = false;
	private String streetrefOSMKey = "";
	private FieldsForUniqueStreet fieldsForUniqueStreet = FieldsForUniqueStreet.STREET;
	private java.util.Date osmdbTimestamp = null;
	private java.util.Date evaluationTimestamp = null;
	private boolean evaluationTypeFull = false;
	private String evaluationText = "";

	public static final Logger logger = Logger.getLogger(EvaluationNew.class.getName());
	
	public enum FieldsForUniqueStreet {
		STREET, STREET_REF, STREET_POSTCODE, STREET_POSTCODE_REF;
	}
	
	public enum StreetNameIdenticalLevel {
		ROUGHLY, EXACTLY;
	}

	public StreetNameIdenticalLevel getStreetnameIdenticalLevel() {
		return streetnameIdenticalLevel;
	}

	public String getCountry() {
		return country;
	}

	public String getMunicipality() {
		return municipality;
	}

	public Long getCountryDbId() {
		return this.countryDbId;
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
	
	public Long getmunicipalityDbId() {
		return municipalityDbId;
	}
	
	public String getMunitipalityGeometryBinaryString() {
		return municipalityGeometryBinaryString;
	}
	
	public FieldsForUniqueStreet getfieldsForUniqueStreet() {
		return fieldsForUniqueStreet;
	}
	
	public java.util.Date getOsmdbTimestamp() {
		return this.osmdbTimestamp;
	}
	
	public java.util.Date getEvaluationTimestamp() {
		return this.evaluationTimestamp;
	}

	public boolean getEvaluationTypeFull() {
		return this.evaluationTypeFull;
	}

	public String getEvaluationText() {
		return this.evaluationText;
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

	public void setCountryDbId(Long countryid) {
		this.countryDbId = countryid;
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

	public void setmunicipalityDbId(Long id) {
		this.municipalityDbId = id;
	}
	
	public void setMunitipalityGeometryBinaryString(String geometrystring) {
		this.municipalityGeometryBinaryString = geometrystring;
	}

	public void setfieldsForUniqueStreet(FieldsForUniqueStreet fields) {
		this.fieldsForUniqueStreet = fields;
	}
	
	public void setOsmdbTimestamp(java.util.Date timestamp) {
		this.osmdbTimestamp = timestamp;
	}
	
	public void setEvaluationTimestamp(java.util.Date timestamp) {
		this.evaluationTimestamp = timestamp;
	}
		
	public void setEvaluationTypeFull(boolean full) {
		this.evaluationTypeFull = full;
	}

	public void setEvaluationText(String text) {
		this.evaluationText = text;
	}
	
	
	private void storeEvaluation(StreetCollection streets) {

Integer evaluationOverviewId = -1;		
		
		DateFormat time_formatter_iso8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");		// in iso8601 format, with timezone
		DateFormat time_formatter_iso8601wozone = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");		// in iso8601 format, without timezone
		DateFormat time_formatter_dateonly = new SimpleDateFormat("yyyy-MM-dd");

		Integer streets_list_count = 0;
		Integer streets_listonly_count = 0;
		Integer streets_identical_count = 0;
		Integer streets_osmonly_count = 0;

		try {
			Integer next_full_overview_id = -1;
			if((1==0) && (this.getEvaluationTypeFull())) {

				if(this.getEvaluationText().equals("")) {
					java.util.Date temp_time = new java.util.Date();
					this.setEvaluationText(time_formatter_dateonly.format(temp_time));
					logger.log(Level.FINE, "test zeit onlydate ==="+this.getEvaluationText()+"===");
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
				insertbefehl_evalationoverview += "VALUES (-1, -1, '" + this.getEvaluationText() +"', 'full', " + next_full_overview_id + ");";
				logger.log(Level.FINE, "insertbefehl_evalationoverview ==="+insertbefehl_evalationoverview+"===");
				Statement stmt_insertevalationoverview = con_listofstreets.createStatement();
				String[] dbautogenkeys = { "id" };
				try {
					stmt_insertevalationoverview.executeUpdate( insertbefehl_evalationoverview, dbautogenkeys );

					ResultSet rs_getautogenkeys = stmt_insertevalationoverview.getGeneratedKeys();
				    while (rs_getautogenkeys.next()) {
				    	logger.log(Level.FINE, "Key returned from getGeneratedKeys():"
				            + rs_getautogenkeys.getInt(1));
				    	evaluationOverviewId = rs_getautogenkeys.getInt("id");
				        logger.log(Level.FINE, "got new id from table evalation_overview, id ===" + evaluationOverviewId + "===");
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

			
			
			String streetresultInsertSql = "INSERT INTO evaluation_street"
				+ " (evaluation_id, street_id, name, streetref, osm_id, osm_type, osm_keyvalue)"
					+ " VALUES (?, ?, ?, ?, ?, ?, ?);";
			PreparedStatement streetresultInsertStmt = con_listofstreets.prepareStatement(streetresultInsertSql);
			System.out.println("Insert street result string ===" + streetresultInsertSql + "===");
			logger.log(Level.FINEST, "insert sql for evaluation_street row ===" + streetresultInsertSql + "===");

				
			Long evaluationId = -1L;
			String evaluationInsertSql = "INSERT INTO evaluation (country_id, municipality_id, evaluation_overview_id,"
				+ " number_liststreets, number_osmstreets, number_osmsinglestreets,number_missingstreets,tstamp, osmdb_tstamp)"
				+ " VALUES (?, ?, ?, 0, 0, 0, 0, ?::TIMESTAMPTZ, ?::TIMESTAMPTZ);";

			String[] dbautogenkeys = { "id" };
			PreparedStatement evaluationInsertStmt = con_listofstreets.prepareStatement(evaluationInsertSql, dbautogenkeys);
			System.out.println("Insert evaluation string ===" + evaluationInsertSql + "===");
			logger.log(Level.FINEST, "insert sql for evaluation ===" + evaluationInsertSql + "===");
			evaluationInsertStmt.setLong(1, this.getCountryDbId());
			evaluationInsertStmt.setLong(2, this.getmunicipalityDbId());
			evaluationInsertStmt.setLong(3, evaluationOverviewId);
			evaluationInsertStmt.setString(4, time_formatter_iso8601wozone.format(this.getEvaluationTimestamp()));
			evaluationInsertStmt.setString(5, time_formatter_iso8601wozone.format(this.getOsmdbTimestamp()));

			String evaluationUpdateSql = "UPDATE evaluation SET"
				+ " number_liststreets = ?, number_osmstreets = ?, number_missingstreets = ?,"
				+ " number_osmsinglestreets = ?  WHERE id = ?;";
			logger.log(Level.FINE, "update_evaluation ===" + evaluationUpdateSql + "===");
			PreparedStatement evaluationUpdateStmt = con_listofstreets.prepareStatement(evaluationUpdateSql);

				
System.out.println("para 4 ===" + time_formatter_iso8601wozone.format(this.getEvaluationTimestamp()) + "===");
System.out.println("para 5 ===" + time_formatter_iso8601wozone.format(this.getOsmdbTimestamp()) + "===");
			
			try {
				evaluationInsertStmt.execute();
				ResultSet evaluationInsertRS = evaluationInsertStmt.getGeneratedKeys();
			    while (evaluationInsertRS.next()) {
			    	logger.log(Level.FINEST, "Key returned from getGeneratedKeys():"
			            + evaluationInsertRS.getLong(1));
			    	evaluationId = evaluationInsertRS.getLong("id");
			    }
			    evaluationInsertRS.close();
			}
			catch( SQLException e) {
				logger.log(Level.INFO, "ERROR: during insert in table evaluation_overview, insert code was ===" + evaluationInsertSql + "===");
				logger.log(Level.INFO, e.toString());
				System.out.println("ERROR: during insert in table evaluation_overview, insert code was ===" + evaluationInsertSql + "===");
				System.out.println(e.toString());
			}
		    evaluationInsertStmt.close();

			con_listofstreets.setAutoCommit(false);


	    	for (Map.Entry<String,Street> entry : streets.cache.entrySet()) {
				Street activestreet = entry.getValue();
				//if(housenumber.getstate().equals("unchanged"))
				//count++;
	
				if(activestreet.osm_id.equals("")) {
				} else if(( ! activestreet.osm_id.equals("")) && (activestreet.street_id.equals(""))) {
					if(	(activestreet.osm_objectkeyvalue != null) &&
						( ! activestreet.osm_objectkeyvalue.equals("")) &&
						(activestreet.osm_objectkeyvalue.indexOf("place=") == 0)) {		// place= nur-osm-Objekte nicht aufführen, weder in nur-osm-Zahl noch mit Namen
					} else {
					}
				} else if(( ! activestreet.osm_id.equals("")) && ( ! activestreet.street_id.equals(""))) {
				}
	
				if(activestreet.osm_id.equals("")) {
					if( ! activestreet.street_id.equals("")) {
						streets_list_count++;
						streets_listonly_count++;
					}
				}	// end of at least one osm-id available, so got odbl-state of all  - if(activestreet.osm_id.equals("")) {
				else {
					if( ! activestreet.street_id.equals("")) {
						streets_identical_count++;
						streets_list_count++;
					} else {
						streets_osmonly_count++;
					}
				}

Long local_street_id = -1L;
				if( ! activestreet.street_id.equals("")) {
					if(activestreet.street_id.indexOf(",") != -1) {
						System.out.println("WARNING: more than one street row found, take only first one ==="
							+ activestreet.street_id.substring(0,activestreet.street_id.indexOf(","))+"=== from list ==="+activestreet.street_id+"===");
						local_street_id = Long.parseLong(activestreet.street_id.substring(0,activestreet.street_id.indexOf(",")));
					} else {
						local_street_id = Long.parseLong(activestreet.street_id);
					}
				} else {
System.out.println("missing street_id at actual street ===" + activestreet.name + "===");
				}

				streetresultInsertStmt.setLong(1, evaluationId);
				streetresultInsertStmt.setLong(2, local_street_id);
				streetresultInsertStmt.setString(3, activestreet.name);
				streetresultInsertStmt.setString(4, activestreet.streetref);
				streetresultInsertStmt.setString(5, activestreet.osm_id);
				streetresultInsertStmt.setString(6, activestreet.osm_type);
				streetresultInsertStmt.setString(7, activestreet.osm_objectkeyvalue);
				streetresultInsertStmt.execute();
	    	}
			streetresultInsertStmt.close();

				// transaction commit
			con_listofstreets.commit();
				// re-activate standard auto-transation mode for every db-action
			con_listofstreets.setAutoCommit(true);

			evaluationUpdateStmt.setInt(1, streets_list_count);
			evaluationUpdateStmt.setInt(2, streets_identical_count);
			evaluationUpdateStmt.setInt(3, streets_listonly_count);
			evaluationUpdateStmt.setInt(4, streets_osmonly_count);
			evaluationUpdateStmt.setLong(5, evaluationId);
	System.out.println("1 ===" + streets_list_count + "===");
	System.out.println("2 ===" + streets_identical_count + "===");
	System.out.println("3 ===" + streets_listonly_count + "===");
	System.out.println("4 ===" + streets_osmonly_count + "===");
	System.out.println("5 ===" + evaluationId + "===");
			try {
				evaluationUpdateStmt.execute();
			}
			catch( SQLException e) {
				logger.log(Level.INFO, "ERROR: during insert in table evaluation, insert code was ===" + evaluationUpdateSql + "===");
				logger.log(Level.FINE, e.toString());
				System.out.println("ERROR: during insert in table evaluation, insert code was ===" + evaluationUpdateSql + "===");
				System.out.println(e.toString());
			}
			evaluationUpdateStmt.close();


			System.out.println("strassen_soll_anzahl: " + streets_list_count);
			System.out.println("streets_listonly_count: " + streets_listonly_count);
			System.out.println("streets_osmonly_count: " + streets_osmonly_count);
		}
		catch( SQLException e) {
			logger.log(Level.INFO, e.toString());
			System.out.println(e.toString());
			return;
		}
	}


	static Connection con_listofstreets = null;
	static Connection con_mapnik = null;
	static Applicationconfiguration configuration = new Applicationconfiguration();
	
	public static void main(String args[]) {
		Boolean evaluationTypeFull = false;
		String evaluationText = "";

		
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
			StreetCollection mergedstreets = new StreetCollection();

			java.util.Date time_act_municipality_starttime = null;
			java.util.Date time_last_step = null;
			java.util.Date time_now = null;

			java.util.Date time_evaluation = null;
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
					} else if(args[argsi].equals("-gemeindeschluessel")) {
						if( ! sqlbefehl_jobs_whereclause.equals(""))
							sqlbefehl_jobs_whereclause += "AND ";
						sqlbefehl_jobs_whereclause += "officialkeys_id = '"+args[argsi+1]+"'";
						args_ok_count += 2;
					} else if(args[argsi].equals("-polygonstate")) {
						parameter_polygonstate = args[argsi+1];
						if( ! sqlbefehl_jobs_whereclause.equals(""))
							sqlbefehl_jobs_whereclause += "AND ";
						sqlbefehl_jobs_whereclause += "polygon_state = '"+args[argsi+1]+"'";
						args_ok_count += 2;
					} else if(args[argsi].equals("-country")) {
						logger.log(Level.FINE, "\ncountry parameter encoded ==="+URLDecoder.decode(args[argsi+1], "UTF-8")+"===");
						if( ! sqlbefehl_jobs_whereclause.equals(""))
							sqlbefehl_jobs_whereclause += "AND ";
						sqlbefehl_jobs_whereclause += "country = '"
							+ URLDecoder.decode(args[argsi+1], "UTF-8") + "' ";
						args_ok_count += 2;
					} else if(args[argsi].equals("-municipality") || args[argsi].equals("-name")) {
						logger.log(Level.FINE, "\ncountry name encoded ==="+URLDecoder.decode(args[argsi+1], "UTF-8")+"===");
						if( ! sqlbefehl_jobs_whereclause.equals(""))
							sqlbefehl_jobs_whereclause += "AND ";
						sqlbefehl_jobs_whereclause += "name = '"
							+ URLDecoder.decode(args[argsi+1], "UTF-8") + "' ";
						args_ok_count += 2;
					} else if(args[argsi].equals("-municipalityhierarchy")) {
						if( ! sqlbefehl_jobs_whereclause.equals(""))
							sqlbefehl_jobs_whereclause += "AND ";
						sqlbefehl_jobs_whereclause += "osm_hierarchy like '%"+args[argsi+1]+"%'";
						args_ok_count += 2;
					} else if(args[argsi].equals("-evaluationtext")) {
						evaluationText = args[argsi+1];
						args_ok_count += 2;
					} else if(args[argsi].equals("-evaluationtype")) {
						if(args[argsi+1].equals("full")) {
							evaluationTypeFull = true;
							logger.log(Level.FINE, "ok, evaltype was correctly 'full'");
							System.out.println("ok, evaltype was correctly 'full'");
						} else {
							logger.log(Level.SEVERE, "ERROR: evaltype was wrong '"+args[argsi+1]+"'");
							System.out.println("ERROR: evaltype was wrong '"+args[argsi+1]+"'");
						}
						args_ok_count += 2;
					} else {
						System.out.println("unknown Parameter ===" + args[argsi] + "===");
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

				time_evaluation = new java.util.Date();

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
					mergedstreets.clear();
						
					if(rs_jobs.getString("countryname").equals("Brasil"))
						evaluation.setStreetnameIdenticalLevel(StreetNameIdenticalLevel.ROUGHLY);
					else
						evaluation.setStreetnameIdenticalLevel(StreetNameIdenticalLevel.EXACTLY);
					evaluation.setCountry(rs_jobs.getString("countryname"));
					evaluation.setCountryDbId(rs_jobs.getLong("country_id"));
					evaluation.setMunicipality(rs_jobs.getString("name"));
					evaluation.setOfficialkeysId(rs_jobs.getString("officialkeys_id"));
					evaluation.setPolgonstate(rs_jobs.getString("polygon_state"));
					if(		(rs_jobs.getString("parameterstreetref") != null) 
						&& 	(! rs_jobs.getString("parameterstreetref").equals(""))) {
						evaluation.setStreetrefMustBeUsedForIdenticaly(rs_jobs.getString("parameterstreetref"));
						evaluation.setfieldsForUniqueStreet(FieldsForUniqueStreet.STREET_REF);
					} else {
						evaluation.unsetStreetrefMustBeUsedForIdenticaly();
						evaluation.setfieldsForUniqueStreet(FieldsForUniqueStreet.STREET);
					}
					evaluation.setPolgonstate(rs_jobs.getString("polygon_state"));
					evaluation.setmunicipalityDbId(rs_jobs.getLong("id"));
					evaluation.setMunitipalityGeometryBinaryString(rs_jobs.getString("polygon"));
					evaluation.setOsmdbTimestamp(time_osmdb);
					evaluation.setEvaluationTimestamp(time_evaluation);
					evaluation.setEvaluationTypeFull(evaluationTypeFull);
					evaluation.setEvaluationText(evaluationText);


					osmstreets = osmreader.ReadDataFromDB(evaluation);
					//liststreets = listreader.ReadListFromDB(evaluation);
					//evaluatedstreets = liststreets.merge(osmstreets);

					listreader.setExistingStreetlist(osmstreets);
					mergedstreets = listreader.ReadListFromDB(evaluation);

					evaluation.storeEvaluation(mergedstreets);

					logger.log(Level.FINE, "========================================================================================");
				}
			}	// end of loop over all municiapalities
			rs_jobs.close();

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