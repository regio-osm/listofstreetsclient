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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.*;
import java.util.Date;
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
	private Date osmdbTimestamp = null;
	private Date evaluationTimestamp = null;
	private boolean evaluationTypeFull = false;
	private String evaluationText = "";
	private Integer fullEvaluationId = -1;

	public static final Logger logger = Logger.getLogger(EvaluationNew.class.getName());
	
	static Connection con_listofstreets = null;
	static Connection con_mapnik = null;
	static Applicationconfiguration configuration = new Applicationconfiguration();
	
	public enum FieldsForUniqueStreet {
		STREET, STREET_REF, STREET_POSTCODE, STREET_POSTCODE_REF;
	}
	
	public enum StreetNameIdenticalLevel {
		ROUGHLY, EXACTLY;
	}

	/**
	 * Initialise class properties
	 * Attention: property fullEvaluationId will not be reseted
	 */
	public void initialize() {
		streetnameIdenticalLevel = StreetNameIdenticalLevel.EXACTLY;
		country = "";
		countryDbId = -1L;
		municipality = "";
		municipalityDbId = -1L;
		municipalityGeometryBinaryString = "";
		officialkeysId = "";
		polygonstate = "";
		streetrefMustBeUsedForIdenticaly = false;
		streetrefOSMKey = "";
		fieldsForUniqueStreet = FieldsForUniqueStreet.STREET;
		osmdbTimestamp = null;
		evaluationTimestamp = null;
		evaluationTypeFull = false;
		evaluationText = "";
	}


	
	/**
	 * 
	 */
	public void close() {
			try {
				if(con_listofstreets != null) {
					logger.log(Level.INFO, "Connection to DB " + con_listofstreets.getMetaData().getURL() + " will be closed.");
					con_listofstreets.close();
				}
				if(con_mapnik != null) {
					logger.log(Level.INFO, "Connection to local DB " + con_mapnik.getMetaData().getURL() + " will be closed.");
					con_mapnik.close();
				}
			}
			catch( SQLException sqle) {
				logger.log(Level.SEVERE, "SQL-Exception occured, when tried to close DB Connection, details follow ..");
				logger.log(Level.SEVERE, sqle.toString());
				System.out.println("SQL-Exception occured, when tried to close DB Connection, details follow ..");
				System.out.println(sqle.toString());
			}
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
	
	public Date getOsmdbTimestamp() {
		return this.osmdbTimestamp;
	}
	
	public Date getEvaluationTimestamp() {
		return this.evaluationTimestamp;
	}

	public boolean getEvaluationTypeFull() {
		return this.evaluationTypeFull;
	}

	public Integer getFullEvaluationId() {
		return this.fullEvaluationId;
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
	
	public void setOsmdbTimestamp(Date timestamp) {
		this.osmdbTimestamp = timestamp;
	}
	
	public void setFullEvaluationId(Integer id) {
		this.fullEvaluationId = id;
	}
	
	public void setEvaluationTimestamp(Date timestamp) {
		this.evaluationTimestamp = timestamp;
	}
		
	public void setEvaluationTypeFull(boolean full) {
		this.evaluationTypeFull = full;
	}

	public void setEvaluationText(String text) {
		this.evaluationText = text;
	}

	
	
	private void storeEvaluation(StreetCollection streets) {

		
		DateFormat time_formatter_iso8601wozone = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");		// in iso8601 format, without timezone
		DateFormat time_formatter_dateonly = new SimpleDateFormat("yyyy-MM-dd");

		Integer streets_list_count = 0;
		Integer streets_listonly_count = 0;
		Integer streets_identical_count = 0;
		Integer streets_osmonly_count = 0;

		logger.log(Level.INFO, "   ----------------------------------------------------------------------------------------");
		logger.log(Level.INFO, "   ----------------------------------------------------------------------------------------");
		logger.log(Level.INFO, "      Store Evaluation Data - Job  =" + getMunicipality() +"=   muni-id: "+getmunicipalityDbId());
		
		try {
			if(this.getEvaluationTypeFull()) {


				if(getFullEvaluationId() == -1) {
					if(this.getEvaluationText().equals("")) {
						Date temp_time = new Date();
						this.setEvaluationText(time_formatter_dateonly.format(temp_time));
						logger.log(Level.FINE, "test zeit onlydate ==="+this.getEvaluationText()+"===");
					}

					Integer local_next_evaluationumber = -1;
					String sql_max_full_overview_id = "SELECT MAX(evaluation_number) AS max_full_number FROM evaluation_overview where evaluation_type = 'full';";
					logger.log(Level.FINE, "hole SQL-Statement sql_max_full_overview_id ==="+sql_max_full_overview_id+"===");
					Statement stmt_max_full_overview_id = con_listofstreets.createStatement();
					ResultSet rs_max_full_overview_id = stmt_max_full_overview_id.executeQuery( sql_max_full_overview_id );
					if(rs_max_full_overview_id.next()) {
						local_next_evaluationumber = 1 + rs_max_full_overview_id.getInt("max_full_number");
						logger.log(Level.FINE, "got already used max number for full evaluation type ===" + local_next_evaluationumber + "===");
					}
		
					String insertbefehl_evalationoverview = "INSERT INTO evaluation_overview (evaluation_first_id, evaluation_last_id, description, evaluation_type, evaluation_number) ";
					insertbefehl_evalationoverview += "VALUES (-1, -1, ?, 'full', ?);";
					logger.log(Level.FINE, "insertbefehl_evalationoverview ==="+insertbefehl_evalationoverview+"===");
					String[] dbautogenkeys = { "id" };
					PreparedStatement stmt_insertevalationoverview = con_listofstreets.prepareStatement(insertbefehl_evalationoverview, dbautogenkeys);
					stmt_insertevalationoverview.setString(1, this.getEvaluationText());
					stmt_insertevalationoverview.setLong(2, local_next_evaluationumber);
					stmt_insertevalationoverview.execute();

					ResultSet insertevalationoverviewRS = stmt_insertevalationoverview.getGeneratedKeys();
				    while (insertevalationoverviewRS.next()) {
				    	logger.log(Level.FINEST, "Key returned from getGeneratedKeys():"
				            + insertevalationoverviewRS.getLong(1));
				    	setFullEvaluationId(insertevalationoverviewRS.getInt("id"));
				    }
				    insertevalationoverviewRS.close();
				    stmt_insertevalationoverview.close();
				}
			}

/*
			String deletesqlstring = "SELECT id FROM street WHERE municipality_id = "+municipality_id+";";
			System.out.println("sql-delete statement for all streets of actual municipality-ID: "+municipality_id+"     ==="+deletesqlstring+"===");
			stmt.executeUpdate( deletesqlstring );
			rs_municipality = stmt.executeQuery( sqlbefehl_municipality );
			System.out.println("streets deleted.");
*/			
			
			String streetresultwithGeomInsertSql = "INSERT INTO evaluation_street"
				+ " (evaluation_id, street_id, name, streetref, osm_id, osm_type, osm_keyvalue, osm_point_leftbottom, osm_point_righttop)"
				+ " VALUES (?, ?, ?, ?, ?, ?, ?, ST_Geomfromtext(?, 4326), ST_Geomfromtext(?, 4326));";
			PreparedStatement streetresultwithGeomInsertStmt = con_listofstreets.prepareStatement(streetresultwithGeomInsertSql);
			logger.log(Level.FINEST, "insert sql for evaluation_street row withGeom ===" + streetresultwithGeomInsertSql + "===");

			String streetresultwithoutGeomInsertSql = "INSERT INTO evaluation_street"
				+ " (evaluation_id, street_id, name, streetref, osm_id, osm_type, osm_keyvalue, osm_point_leftbottom, osm_point_righttop)"
				+ " VALUES (?, ?, ?, ?, ?, ?, ?, null, null);";
			PreparedStatement streetresultwithoutGeomInsertStmt = con_listofstreets.prepareStatement(streetresultwithoutGeomInsertSql);
			logger.log(Level.FINEST, "insert sql for evaluation_street row withoutGeom ===" + streetresultwithoutGeomInsertSql + "===");

			Long evaluationId = -1L;
			String evaluationInsertSql = "INSERT INTO evaluation (country_id, municipality_id, evaluation_overview_id,"
				+ " number_liststreets, number_osmstreets, number_osmsinglestreets,number_missingstreets,tstamp, osmdb_tstamp)"
				+ " VALUES (?, ?, ?, 0, 0, 0, 0, ?::TIMESTAMPTZ, ?::TIMESTAMPTZ);";

			String[] dbautogenkeys = { "id" };
			PreparedStatement evaluationInsertStmt = con_listofstreets.prepareStatement(evaluationInsertSql, dbautogenkeys);
			logger.log(Level.FINEST, "insert sql for evaluation ===" + evaluationInsertSql + "===");
			evaluationInsertStmt.setLong(1, this.getCountryDbId());
			evaluationInsertStmt.setLong(2, this.getmunicipalityDbId());
			evaluationInsertStmt.setInt(3, this.getFullEvaluationId());
			evaluationInsertStmt.setString(4, time_formatter_iso8601wozone.format(this.getEvaluationTimestamp()));
			evaluationInsertStmt.setString(5, time_formatter_iso8601wozone.format(this.getOsmdbTimestamp()));
			logger.log(Level.FINE, "evaluationInsertStmt: [country_id] " + this.getCountryDbId() + "  [muni_id] " + this.getmunicipalityDbId()
				+ "  [eval_overview_id] " + getFullEvaluationId() + "  [eval_tstamp] " + time_formatter_iso8601wozone.format(this.getEvaluationTimestamp())
				+ "  [osmdb_tstamp] " +time_formatter_iso8601wozone.format(this.getOsmdbTimestamp()));

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


			String evaluationUpdateSql = "UPDATE evaluation SET"
				+ " number_liststreets = ?, number_osmstreets = ?, number_missingstreets = ?,"
				+ " number_osmsinglestreets = ?  WHERE id = ?;";
			logger.log(Level.FINEST, "update_evaluation ===" + evaluationUpdateSql + "===");
			PreparedStatement evaluationUpdateStmt = con_listofstreets.prepareStatement(evaluationUpdateSql);


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
						logger.log(Level.INFO, "WARNING: more than one street row found, take only first one ==="
							+ activestreet.street_id.substring(0,activestreet.street_id.indexOf(","))+"=== from list ==="+activestreet.street_id+"===");
						local_street_id = Long.parseLong(activestreet.street_id.substring(0,activestreet.street_id.indexOf(",")));
					} else {
						local_street_id = Long.parseLong(activestreet.street_id);
					}
				} else {
					logger.log(Level.FINE, "missing street_id at actual street ===" + activestreet.name + "===");
				}

				if(		(activestreet.point_leftbottom != null) && !activestreet.point_leftbottom.equals("")
					&&	(activestreet.point_righttop != null) && !activestreet.point_righttop.equals("")) {
					streetresultwithGeomInsertStmt.setLong(1, evaluationId);
					streetresultwithGeomInsertStmt.setLong(2, local_street_id);
					streetresultwithGeomInsertStmt.setString(3, activestreet.name);
					streetresultwithGeomInsertStmt.setString(4, activestreet.streetref);
					streetresultwithGeomInsertStmt.setString(5, activestreet.osm_id);
					streetresultwithGeomInsertStmt.setString(6, activestreet.osm_type);
					streetresultwithGeomInsertStmt.setString(7, activestreet.osm_objectkeyvalue);
					streetresultwithGeomInsertStmt.setString(8, activestreet.point_leftbottom);
					streetresultwithGeomInsertStmt.setString(9, activestreet.point_righttop);
					logger.log(Level.FINE, "streetresultwithGeomInsertStmt: [eval-id] " + evaluationId + "  [street_id] " + local_street_id
						+ "  [streetname] " + activestreet.name + "  [streetref] " + activestreet.streetref + "  [osm_id] " + activestreet.osm_id
						+ "  [osm_typ] " + activestreet.osm_type + "  [osm_keyvalues] " + activestreet.osm_objectkeyvalue 
						+ "  [bbox-lb] " + activestreet.point_leftbottom + "  [bbox-rt] " +activestreet.point_righttop);
					streetresultwithGeomInsertStmt.execute();
				} else {
					streetresultwithoutGeomInsertStmt.setLong(1, evaluationId);
					streetresultwithoutGeomInsertStmt.setLong(2, local_street_id);
					streetresultwithoutGeomInsertStmt.setString(3, activestreet.name);
					streetresultwithoutGeomInsertStmt.setString(4, activestreet.streetref);
					streetresultwithoutGeomInsertStmt.setString(5, activestreet.osm_id);
					streetresultwithoutGeomInsertStmt.setString(6, activestreet.osm_type);
					streetresultwithoutGeomInsertStmt.setString(7, activestreet.osm_objectkeyvalue);
					logger.log(Level.FINE, "streetresultwithoutGeomInsertStmt: [eval-id] " + evaluationId + "  [street_id] " + local_street_id
							+ "  [streetname] " + activestreet.name + "  [streetref] " + activestreet.streetref + "  [osm_id] " + activestreet.osm_id
							+ "  [osm_typ] " + activestreet.osm_type + "  [osm_keyvalues] " + activestreet.osm_objectkeyvalue);
					streetresultwithoutGeomInsertStmt.execute();
				}
	    	}
			streetresultwithGeomInsertStmt.close();
			streetresultwithoutGeomInsertStmt.close();

			logger.log(Level.INFO, "Start commit of insert street results ...");
				// transaction commit
			con_listofstreets.commit();
				// re-activate standard auto-transation mode for every db-action
			con_listofstreets.setAutoCommit(true);
			logger.log(Level.INFO, "Finished commit of insert street results");

			try {
				evaluationUpdateStmt.setInt(1, streets_list_count);
				evaluationUpdateStmt.setInt(2, streets_identical_count);
				evaluationUpdateStmt.setInt(3, streets_listonly_count);
				evaluationUpdateStmt.setInt(4, streets_osmonly_count);
				evaluationUpdateStmt.setLong(5, evaluationId);
				logger.log(Level.FINE, "evaluationUpdateStmt: [str_list_count] " + streets_list_count + "  [str_ident_count] " + streets_identical_count
					+ "  [str_listonly_count] " + streets_listonly_count + "  [str_osmonly_count] " + streets_osmonly_count
					+ "  [eval_id] " + evaluationId);
				evaluationUpdateStmt.execute();
			}
			catch( SQLException e) {
				logger.log(Level.SEVERE, "ERROR: during insert in table evaluation, insert code was ===" + evaluationUpdateSql + "===");
				logger.log(Level.SEVERE, e.toString());
				System.out.println("ERROR: during insert in table evaluation, insert code was ===" + evaluationUpdateSql + "===");
				System.out.println(e.toString());
			}
			evaluationUpdateStmt.close();


			logger.log(Level.INFO, "strassen_soll_anzahl: " + streets_list_count);
			logger.log(Level.INFO, "streets_listonly_count: " + streets_listonly_count);
			logger.log(Level.INFO, "streets_osmonly_count: " + streets_osmonly_count);
		}
		catch( SQLException e) {
			logger.log(Level.INFO, e.toString());
			System.out.println(e.toString());
			return;
		}
		logger.log(Level.INFO, "   ----------------------------------------------------------------------------------------");
		logger.log(Level.INFO, "   ----------------------------------------------------------------------------------------");
	}


	
	public static void main(String args[]) {
		Boolean evaluationTypeFull = false;
		String evaluationText = "";

		
		Date time_program_startedtime = new Date();
		Long osmdata_duration_ms = 0L;
		Long listdata_duration_ms = 0L;
		Long storedata_duration_ms = 0L;
		DateFormat time_formatter_mesz = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss z");
		DateFormat time_formatter_iso8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");		// in iso8601 format, with timezone
		String parameterConfiguration = "";


		try {

			Handler handler = new ConsoleHandler();
			handler.setLevel(configuration.logging_console_level);
			logger.addHandler( handler );
			SimpleFormatter simpleformatter = new SimpleFormatter();
			FileHandler fhandler = new FileHandler(configuration.logging_filename);
			fhandler.setLevel(configuration.logging_file_level);
			fhandler.setFormatter(simpleformatter);
			logger.addHandler( fhandler );
			logger.setLevel(configuration.logging_console_level);
		} 
		catch (IOException e) {
			System.out.println("Fehler beim Logging-Handler erstellen, ...");
			System.out.println(e.toString());
		}

		
		logger.log(Level.INFO, "Program: Started Time: "+time_formatter_mesz.format(time_program_startedtime));
		
		
		
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
			System.out.println("-configuration filenameorabsolutepathandfilename");
			return;
		}

		if(args.length >= 1) {
			int args_ok_count = 0;
			for(int argsi=0;argsi<args.length;argsi+=2) {
				if(args[argsi].equals("-configuration")) {
					logger.log(Level.FINE, " args pair analysing #: "+argsi+"  ==="+args[argsi]+"===   #+1: "+(argsi+1)+"   ==="+args[argsi+1]+"===");
					parameterConfiguration = args[argsi+1];
					args_ok_count += 2;
				}
			}
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

			OsmDataReader osmreader = new OsmDataReader();
			StreetlistReader listreader = new StreetlistReader();
			EvaluationNew evaluation = new EvaluationNew();

			StreetCollection osmstreets = new StreetCollection();
			StreetCollection mergedstreets = new StreetCollection();


			Date time_evaluation = null;
			Date time_osmdb = null;



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
						return;
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

			if(!parameterConfiguration.equals("")) {
				logger.log(Level.INFO, "read excplicit given configuration file ===" + parameterConfiguration + "===");
				configuration = new Applicationconfiguration(parameterConfiguration);
			}

			String osmosis_laststatefile = configuration.osmosis_laststatefile;

			logger.log(Level.FINE, "hole SQL-Statement Job-Suche ==="+sqlbefehl_jobs+"===");
			Statement stmt_jobs = con_listofstreets.createStatement();
			ResultSet rs_jobs = stmt_jobs.executeQuery( sqlbefehl_jobs );
			String akt_jobname = "";
			Integer municipality_count = 0;
			while(rs_jobs.next()) {

				time_evaluation = new Date();

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
							Date temp_time = new Date();
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
				
				municipality_count++;

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

					osmreader.initialize();
					listreader.initialize();
					evaluation.initialize();
					
						
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

					logger.log(Level.FINE, "========================================================================================");
					logger.log(Level.FINE, " # " + municipality_count + "  Job  =" + evaluation.getMunicipality() +"=   muni-id: "+evaluation.getmunicipalityDbId());
					logger.log(Level.FINE, "----------------------------------------------------------------------------------------");

					Date local_osmdata_start = new Date();
					osmstreets.clear();
					osmstreets = osmreader.ReadDataFromDB(evaluation);
					Date local_osmdata_end = new Date();
					osmdata_duration_ms += local_osmdata_end.getTime() - local_osmdata_start.getTime();

					Date local_listdata_start = new Date();
					listreader.setExistingStreetlist(osmstreets);
					mergedstreets.clear();
					mergedstreets = listreader.ReadListFromDB(evaluation);
					Date local_listdata_end = new Date();
					listdata_duration_ms += local_listdata_end.getTime() - local_listdata_start.getTime();
//TODO remove duplicate osm street name objects. Happens, if not only name=*, but also name-Variation were stored in StreetCollection - mergedstreets = evaluation.removeHitVariation(mergedstreets);

					Date local_storedata_start = new Date();
					evaluation.storeEvaluation(mergedstreets);
					Date local_storedata_end = new Date();
					storedata_duration_ms += local_storedata_end.getTime() - local_storedata_start.getTime();

					logger.log(Level.FINE, "========================================================================================");
				}
			}	// end of loop over all municiapalities
			rs_jobs.close();

				// print time durations after all jobs into log - just for getting most time consuming code parts
			osmreader.printTimeDurations();

			osmreader.close();
			listreader.close();
			evaluation.close();
			
			Date time_program_endedtime = new Date();
			logger.log(Level.INFO, "Program: Ended Time: "+time_formatter_mesz.format(time_program_endedtime));
			logger.log(Level.INFO, "Program: Duration in s: "+(time_program_endedtime.getTime()-time_program_startedtime.getTime())/1000);
			logger.log(Level.INFO, "Part OSM-Data Duration in s: " + (osmdata_duration_ms/1000));
			logger.log(Level.INFO, "Part List-Data Duration in s: " + (listdata_duration_ms/1000));
			logger.log(Level.INFO, "Part store Data Duration in s: " + (storedata_duration_ms/1000));
		}
		catch( SQLException e) {
			logger.log(Level.SEVERE, "SQLException occured in EvaluationNew, details follows ...");
			logger.log(Level.SEVERE, e.toString());
			System.out.println("SQLException occured in EvaluationNew, details follows ...");
			System.out.println(e.toString());
			try {
				con_listofstreets.rollback();
				con_listofstreets.close();
			} catch( SQLException innere) {
				logger.log(Level.SEVERE, "inner sql-exception (tried to rollback transaction or to close connection ...");
				logger.log(Level.SEVERE, innere.toString());
				System.out.println("inner sql-exception (tried to rollback transaction or to close connection ...");
				System.out.println(innere.toString());
			}
			return;
		} catch (UnsupportedEncodingException e) {
			logger.log(Level.SEVERE, "UnsupportedEncodingException occured in EvaluationNew, details follows ...");
			logger.log(Level.SEVERE, e.toString());
			System.out.println("UnsupportedEncodingException occured in EvaluationNew, details follows ...");
			System.out.println(e.toString());
		}
	}
}
