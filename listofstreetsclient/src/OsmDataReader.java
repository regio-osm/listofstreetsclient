

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import de.regioosm.listofstreetsclient.Applicationconfiguration;

public class OsmDataReader {
	static Applicationconfiguration configuration = new Applicationconfiguration();
	private static Logger logger = (Logger) EvaluationNew.logger;
	static Connection con_mapnik = null;

	private Long linesQueryDuration = 0L;
	private Long linesAnalyzeDuration = 0L;
	private Long polygonsQueryDuration = 0L;
	private Long polygonsAnalyzeDuration = 0L;
	private Long pointsQueryDuration = 0L;
	private Long pointsAnalyzeDuration = 0L;
	
	public OsmDataReader() {
		String url_mapnik = "";

		this.initialize();

		pointsQueryDuration = 0L;
		pointsAnalyzeDuration = 0L;
		linesQueryDuration = 0L;
		linesAnalyzeDuration = 0L;
		polygonsQueryDuration = 0L;
		polygonsAnalyzeDuration = 0L;

		try {
			if(con_mapnik == null) {
				url_mapnik = configuration.db_osm2pgsql_url;
				con_mapnik = DriverManager.getConnection(url_mapnik, configuration.db_osm2pgsql_username, configuration.db_osm2pgsql_password);
			}
			logger.log(Level.INFO, "Connection to database " + url_mapnik + " established.");
		}
		catch (SQLException e) {
			logger.log(Level.SEVERE, "ERROR: failed to connect to database " + url_mapnik);
			logger.log(Level.SEVERE, e.toString());
			System.out.println("ERROR: failed to connect to database " + url_mapnik);
			System.out.println(e.toString());
			return;
		}
	}

	/**
	 * initial class properties
	 */
	public void initialize() {
	}


	/**
	 * 
	 */
	public void close() {
			try {
				if(con_mapnik != null) {
					logger.log(Level.INFO, "Connection to mapnik DB " + con_mapnik.getMetaData().getURL() + " will be closed.");
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


	public void printTimeDurations() {
		logger.log(Level.INFO, "Time for lines DB query in msec: " + linesQueryDuration);
		logger.log(Level.INFO, "Time for lines Analyzing in msec: " + linesAnalyzeDuration);
		logger.log(Level.INFO, "Time for polygons DB query in msec: " + polygonsQueryDuration);
		logger.log(Level.INFO, "Time for polygons Analyzing in msec: " + polygonsAnalyzeDuration);
		logger.log(Level.INFO, "Time for points DB query in msec: " + pointsQueryDuration);
		logger.log(Level.INFO, "Time for points Analyzing in msec: " + pointsAnalyzeDuration);
	}
	
	public StreetCollection ReadDataFromDB(EvaluationNew  evaluation) {
		StreetCollection streets = new StreetCollection();

		if(		(evaluation.getCountry().equals(""))
			||	(evaluation.getMunicipality().equals(""))) {
			return new StreetCollection();
		}		

		if(con_mapnik == null) {
			return new StreetCollection();
		}
		
		
		String akt_jobname = evaluation.getMunicipality();
		String akt_parameterstreetref = "";
		if(evaluation.isStreetrefMustBeUsedForIdenticaly()) {
			akt_parameterstreetref = evaluation.getStreetrefOSMKey();
		}
		
		streets.setEvaluationParameters(evaluation);

		String osmosis_laststatefile = configuration.osmosis_laststatefile;

		try {
			String dateizeile = "";
	
			DateFormat time_formatter_iso8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");		// in iso8601 format, with timezone
	
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

				}
			}
			filereader.close();
		}
		catch (Exception e) {
			logger.log(Level.INFO, "ERROR: failed to read osmosis last.state.txt file ==="+osmosis_laststatefile+"===");
			logger.log(Level.INFO, e.toString());
			System.out.println("ERROR: failed to read osmosis last.state.txt file ==="+osmosis_laststatefile+"===");
			System.out.println(e.toString());
			return new StreetCollection();
		}
			


			
		Integer anzahl_datensaetze_osmstreets = 0;
		Integer anzahl_datensaetze_osmplaces = 0;


		logger.log(Level.FINE, "   ----------------------------------------------------------------------------------------");
		logger.log(Level.FINE, "   ----------------------------------------------------------------------------------------");
		logger.log(Level.FINE, "      Read OSM Data - Job  =" + akt_jobname +"=   muni-id: "+evaluation.getmunicipalityDbId());


		String akt_gebietsgeometrie = evaluation.getMunitipalityGeometryBinaryString();

		String sqlbefehl_objekte = "";

			// Array of all name-variants, which will be requested by sql-statement; most important at END
		String[] keyvariation_namelist = {"old_name", "loc_name", "alt_name", "official_name", "name"};

boolean examineStreetGeometryData = false;
			// ------------------------------------------------------------------------------
			// 1. Action - get all street from osm table   PLANET_LINE   within boundary-polygon
						// DISTINCT ON (tags->'name')

		sqlbefehl_objekte = "SELECT";
		for(Integer nameindex = 0; nameindex < keyvariation_namelist.length; nameindex++) {
			String act_name = keyvariation_namelist[nameindex];
			sqlbefehl_objekte += " tags -> ? AS "+act_name+",";
		}
		sqlbefehl_objekte += " osm_id AS id, highway AS highwaytype,"
			+ " tags->'postal_code' AS postalcode,"
			+ " ST_AsText(ST_Transform(ST_Centroid(way),4326)) AS linecenterpoint";
		if(examineStreetGeometryData) {
			sqlbefehl_objekte += ", ST_Xmin(Box2D(ST_Transform(way,4326))) AS leftbottom_xpos,"
				+ " ST_Ymin(Box2D(ST_Transform(way,4326))) AS leftbottom_ypos,"
				+ " ST_Xmax(Box2D(ST_Transform(way,4326))) AS righttop_xpos,"
				+ " ST_Ymax(Box2D(ST_Transform(way,4326))) AS righttop_ypos";
		}
		if(! akt_parameterstreetref.equals(""))
			sqlbefehl_objekte += ", tags-> ? AS streetref";
		else
			sqlbefehl_objekte += ", null AS streetref";
		sqlbefehl_objekte += " FROM planet_line WHERE"
			+ " (ST_Covers(?::geometry, way) OR"	// ST_Covers = complete inside
			+ " ST_Crosses(?::geometry, way)) AND"  // ST_Crosses = some, but not all, common
			+ " exist(tags, 'highway')";
		for(Integer nameindex = 0; nameindex < keyvariation_namelist.length; nameindex++) {
			if(nameindex == 0)
				sqlbefehl_objekte += " AND (";
			sqlbefehl_objekte += " exist(tags, ?)";
			if(nameindex == (keyvariation_namelist.length -1))
				sqlbefehl_objekte += ")";
			else
				sqlbefehl_objekte += " OR";
		}
		sqlbefehl_objekte += " ORDER BY tags->'name';";

		String statementParameters = "";
		
		PreparedStatement lineObjectsStmt = null;
		ResultSet lineObjectsRS = null;
		try {
			//logger.log(Level.FINEST, "sqlbefehl_objekte (osm ways) ==="+sqlbefehl_objekte+"===");
			lineObjectsStmt = con_mapnik.prepareStatement(sqlbefehl_objekte);
			Integer statementIndex = 1;
			statementParameters = "";
			for(Integer nameindex = 0; nameindex < keyvariation_namelist.length; nameindex++) {
				String act_name = keyvariation_namelist[nameindex];
				lineObjectsStmt.setString(statementIndex++, act_name);
				statementParameters += " [" + (statementIndex - 1) + "] ===" + act_name + "===";
			}
			if(! akt_parameterstreetref.equals("")) {
				lineObjectsStmt.setString(statementIndex++, akt_parameterstreetref);
				statementParameters += " [" + (statementIndex - 1) + "] ===" + akt_parameterstreetref+ "===";
			}
			lineObjectsStmt.setString(statementIndex++, akt_gebietsgeometrie);
			statementParameters += " [" + (statementIndex - 1) + "] ===((akt_gebietsgeometrie))===";
			lineObjectsStmt.setString(statementIndex++, akt_gebietsgeometrie);
			statementParameters += " [" + (statementIndex - 1) + "] ===((akt_gebietsgeometrie))===";
			for(Integer nameindex = 0; nameindex < keyvariation_namelist.length; nameindex++) {
				String act_name = keyvariation_namelist[nameindex];
				lineObjectsStmt.setString(statementIndex++, act_name);
				statementParameters += " [" + (statementIndex - 1) + "] ===" + act_name + "===";
			}
			logger.log(Level.FINEST, "Query 1: statementParameters ===" + statementParameters + "===");
			
			Date local_query_start = new Date();

			lineObjectsRS = lineObjectsStmt.executeQuery();

			Date local_query_end = new Date();
			logger.log(Level.FINEST, "TIME single-step quer osm-ways in ms. "+(local_query_end.getTime()-local_query_start.getTime()));
			linesQueryDuration += local_query_end.getTime() - local_query_start.getTime();
			local_query_start = local_query_end;

			try {
				Date step_query_start = new Date();
				while( lineObjectsRS.next() ) {
					anzahl_datensaetze_osmstreets++;
						// get name of object
					String temp_aktstrasse = "";
					String act_name = "";
					for(Integer nameindex = 0; nameindex < keyvariation_namelist.length; nameindex++) {
						act_name = keyvariation_namelist[nameindex];
						if( lineObjectsRS.getString(act_name) != null) {
							temp_aktstrasse = lineObjectsRS.getString(act_name);
							if (!act_name.equals("name")) {
								logger.log(Level.FINER, "Ausgabe OSM-Straße Name-Variation (" + act_name + ") ==="+temp_aktstrasse +"===");
							}
							logger.log(Level.FINER, "OSM-Straße ==="+temp_aktstrasse +"===    osm-id ==="+lineObjectsRS.getString("id")+"===     name-variant: " + act_name);
							String local_osm_validkeyvalue = "";
								// check for all possible values, optionally separated by ;
							String[] local_keyvalue = lineObjectsRS.getString("highwaytype").split(";");
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
									logger.log(Level.FINE, " 1. Action highway unexpected value, please check ==="+local_keyvalue[local_keyvaluei]+"===   street name ==="+temp_aktstrasse+"===  osm-id ==="+lineObjectsRS.getString("id")+"===");
								}
							}
		
							if( ! local_osm_validkeyvalue.equals("")) {
								String temp_linecenterpoint = lineObjectsRS.getString("linecenterpoint");
								String temp_point_source = "";
								if( ! temp_linecenterpoint.equals(""))
									temp_point_source = "OSM";
								String point_leftbottom = null;
								String point_righttop = null;
								if(examineStreetGeometryData) {
									point_leftbottom = "POINT(" + lineObjectsRS.getDouble("leftbottom_xpos") + " " + lineObjectsRS.getDouble("leftbottom_ypos") + ")";
									point_righttop = "POINT(" + lineObjectsRS.getDouble("righttop_xpos") + " " + lineObjectsRS.getDouble("righttop_ypos") + ")";
								}
								Street new_street =  new Street("osm", "way", local_osm_validkeyvalue, lineObjectsRS.getLong("id"), 
										temp_aktstrasse, lineObjectsRS.getString("streetref"), lineObjectsRS.getString("postalcode"),
										temp_linecenterpoint, temp_point_source, point_leftbottom, point_righttop);
								if(streets.get(new_street) != null)
										streets.get(new_street).update(new_street);
								else
										streets.add(new_street);
							}
						}
					}
				}
				lineObjectsRS.close();
				lineObjectsStmt.close();
				Date step_query_end = new Date();
				logger.log(Level.FINEST, "TIME single-step analyzing osm-ways in ms. "+(step_query_end.getTime()-step_query_start.getTime()));
				logger.log(Level.FINE, "Number of streets got from planet_line: " + anzahl_datensaetze_osmstreets);
				linesAnalyzeDuration += step_query_end.getTime() - step_query_start.getTime();
			}
			catch( SQLException sqlerror) {
				logger.log(Level.SEVERE, "ERROR: SQL-Exception during 1. Action - get all way streets from osm, ...");
				logger.log(Level.SEVERE, "Query parameters ===" + statementParameters + "===");
				logger.log(Level.SEVERE, "sql-statement was ==="+sqlbefehl_objekte+"=== municipality   id==="+evaluation.getmunicipalityDbId()+"===  name ==="+evaluation.getMunicipality()+"===");
				logger.log(Level.SEVERE, sqlerror.toString());
	//TODO stored errors, like org.postgresql.util.PSQLException: ERROR: GEOS covers() threw an error!
				System.out.println("ERROR: SQL-Exception during 1. Action - get all way streets from osm, ...");
				System.out.println("Query parameters ===" + statementParameters + "===");
				System.out.println("sql-statement was ==="+sqlbefehl_objekte+"=== municipality   id==="+evaluation.getmunicipalityDbId()+"===  name ==="+evaluation.getMunicipality()+"===");
				System.out.println(sqlerror.toString());
				try {
					if(lineObjectsRS != null)
						lineObjectsRS.close();
					if(lineObjectsStmt != null)
						lineObjectsStmt.close();
				} 
				catch(Exception e) {
					logger.log(Level.SEVERE, "General Exception, Details: " + e.toString());
					System.out.println("General Exception, Details: " + e.toString());
				}
			}
		
		} catch( SQLException sqlerror) {
			logger.log(Level.SEVERE, "ERROR: SQL-Exception during 1. Action - get all way streets from osm, ...");
			logger.log(Level.SEVERE, "Query parameters ===" + statementParameters + "===");
			logger.log(Level.SEVERE, "sql-statement was ==="+sqlbefehl_objekte+"=== municipality   id==="+evaluation.getmunicipalityDbId()+"===  name ==="+evaluation.getMunicipality()+"===");
			logger.log(Level.SEVERE, sqlerror.toString());
//TODO stored errors, like org.postgresql.util.PSQLException: ERROR: GEOS covers() threw an error!
			System.out.println("ERROR: SQL-Exception during 1. Action - get all way streets from osm, ...");
			System.out.println("Query parameters ===" + statementParameters + "===");
			System.out.println("sql-statement was ==="+sqlbefehl_objekte+"=== municipality   id==="+evaluation.getmunicipalityDbId()+"===  name ==="+evaluation.getMunicipality()+"===");
			System.out.println(sqlerror.toString());
		}
	
				
			// ------------------------------------------------------------------------------
			// 1a. Action - get all street from osm table   PLANET_POLYGON   (so only closed ways) within boundary-polygon

		sqlbefehl_objekte = "SELECT ";
		for(Integer nameindex = 0; nameindex < keyvariation_namelist.length; nameindex++) {
			String act_name = keyvariation_namelist[nameindex];
			sqlbefehl_objekte += " tags -> ? AS "+act_name+",";
		}
		sqlbefehl_objekte += " osm_id AS id, highway as highwaytype, tags->'place' as place,"
			+ " tags->'postal_code' AS postalcode,"
			+ " ST_AsText(ST_Transform(ST_Centroid(way),4326)) as linecenterpoint";
		if(examineStreetGeometryData) {
			sqlbefehl_objekte += ", ST_Xmin(Box2D(ST_Transform(way,4326))) AS leftbottom_xpos,"
				+ " ST_Ymin(Box2D(ST_Transform(way,4326))) AS leftbottom_ypos,"
				+ " ST_Xmax(Box2D(ST_Transform(way,4326))) AS righttop_xpos,"
				+ " ST_Ymax(Box2D(ST_Transform(way,4326))) AS righttop_ypos";
		}
		if(! akt_parameterstreetref.equals(""))
			sqlbefehl_objekte += ", tags -> ? AS streetref";
		else
			sqlbefehl_objekte += ", null AS streetref";
		sqlbefehl_objekte += " FROM planet_polygon";
		sqlbefehl_objekte += " WHERE ST_Covers(?::geometry, way) AND";		//ST_Centroid(way) seems to take too long
		sqlbefehl_objekte += " (highway != '' OR place != '')";
		for(Integer nameindex = 0; nameindex < keyvariation_namelist.length; nameindex++) {
			if(nameindex == 0)
				sqlbefehl_objekte += " AND (";
			sqlbefehl_objekte += " exist(tags, ?)";
			if(nameindex == (keyvariation_namelist.length -1))
				sqlbefehl_objekte += ")";
			else
				sqlbefehl_objekte += " OR";
		}
		sqlbefehl_objekte += " ORDER BY name;";

		//logger.log(Level.FINEST, "sqlbefehl_objekte (osm closed ways) ==="+sqlbefehl_objekte+"===");

		PreparedStatement polygonObjectsStmt = null;
		ResultSet polygonObjectsRS = null;
		try {
			polygonObjectsStmt = con_mapnik.prepareStatement(sqlbefehl_objekte);
			Integer statementIndex = 1;
			statementParameters = "";
			for(Integer nameindex = 0; nameindex < keyvariation_namelist.length; nameindex++) {
				String act_name = keyvariation_namelist[nameindex];
				polygonObjectsStmt.setString(statementIndex++, act_name);
				statementParameters += " [" + (statementIndex - 1) + "] ===" + act_name + "===";
			}
			if(! akt_parameterstreetref.equals("")) {
				polygonObjectsStmt.setString(statementIndex++, akt_parameterstreetref);
				statementParameters += " [" + (statementIndex - 1) + "] ===" + akt_parameterstreetref + "===";
			}
			polygonObjectsStmt.setString(statementIndex++, akt_gebietsgeometrie);
			statementParameters += " [" + (statementIndex - 1) + "] ===((akt_gebietsgeometrie))===";
			for(Integer nameindex = 0; nameindex < keyvariation_namelist.length; nameindex++) {
				String act_name = keyvariation_namelist[nameindex];
				polygonObjectsStmt.setString(statementIndex++, act_name);
				statementParameters += " [" + (statementIndex - 1) + "] ===" + act_name + "===";
			}
			logger.log(Level.FINEST, "Query 1: statementParameters ===" + statementParameters + "===");

			Date local_query_start = new Date();
			polygonObjectsRS = polygonObjectsStmt.executeQuery();

			Date local_query_end = new Date();
			logger.log(Level.FINEST, "TIME single-step closed osm-ways in ms. "+(local_query_end.getTime()-local_query_start.getTime()));
			polygonsQueryDuration += local_query_end.getTime() - local_query_start.getTime();
			local_query_start = local_query_end;

		
			try {
				anzahl_datensaetze_osmstreets = 0;
				anzahl_datensaetze_osmplaces = 0;
				Date step_query_start = new Date();
				while( polygonObjectsRS.next() ) {
					String local_osm_validkeyvalue = "";

						// get name of object
					String temp_aktstrasse = "";
					String act_name = "";
					for(Integer nameindex = 0; nameindex < keyvariation_namelist.length; nameindex++) {
						act_name = keyvariation_namelist[nameindex];
						if( polygonObjectsRS.getString(act_name) != null) {
							temp_aktstrasse = polygonObjectsRS.getString(act_name);
							if (!act_name.equals("name")) {
								logger.log(Level.FINER, "Ausgabe OSM-Straße Name-Variation (" + act_name + ") ==="+temp_aktstrasse +"===");
							}

							if(polygonObjectsRS.getString("highwaytype") != null) {
								anzahl_datensaetze_osmstreets++;
			
									// check for all possible values, optionally separated by ;
								String[] local_keyvalue = polygonObjectsRS.getString("highwaytype").split(";");
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
							}
						}
					}  // end of type highway
					if(polygonObjectsRS.getString("place") != null) {
						anzahl_datensaetze_osmplaces++;
						logger.log(Level.FINER, "OSM-Placename ==="+act_name+"===   place-value ==="+polygonObjectsRS.getString("place")+"===   osm-id ==="+polygonObjectsRS.getString("id")+"===");
							// check for all possible values, optionally separated by ;
						String[] local_keyvalue = polygonObjectsRS.getString("place").split(";");
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
						logger.log(Level.FINEST, " found interesting polygon-object with name, osm-keyvalues ===" + local_osm_validkeyvalue + "===");
						String temp_linecenterpoint = polygonObjectsRS.getString("linecenterpoint");
						String temp_point_source = "";
						if( ! temp_linecenterpoint.equals(""))
							temp_point_source = "OSM";
						String point_leftbottom = null;;
						String point_righttop = null;
						if(examineStreetGeometryData) {
							point_leftbottom = "POINT(" + polygonObjectsRS.getDouble("leftbottom_xpos") + " " + polygonObjectsRS.getDouble("leftbottom_ypos") + ")";
							point_righttop = "POINT(" + polygonObjectsRS.getDouble("righttop_xpos") + " " + polygonObjectsRS.getDouble("righttop_ypos") + ")";
						}
						Street new_street =  new Street("osm", "way", local_osm_validkeyvalue, polygonObjectsRS.getLong("id"), 
								temp_aktstrasse, polygonObjectsRS.getString("streetref"), polygonObjectsRS.getString("postalcode"),
								temp_linecenterpoint, temp_point_source, point_leftbottom, point_righttop);
						if(streets.get(new_street) != null)
								streets.update(new_street);
						else
								streets.add(new_street);
					}
				}
				polygonObjectsRS.close();
				polygonObjectsStmt.close();
				Date step_query_end = new Date();
				polygonsAnalyzeDuration += step_query_end.getTime() - step_query_start.getTime();
				logger.log(Level.FINEST, "TIME single-step analyzing osm-ways in ms. "+(step_query_end.getTime()-step_query_start.getTime()));
				logger.log(Level.FINE, "planet_polygon - Number of streets: " + anzahl_datensaetze_osmstreets + "  Number of places: " + anzahl_datensaetze_osmplaces);
			}
			catch( SQLException sqlerror) {
				logger.log(Level.SEVERE, "ERROR: SQL-Exception during 1a. Action - get all closed way streets from osm, ...");
				logger.log(Level.SEVERE, "Query parameters ===" + statementParameters + "===");
				logger.log(Level.SEVERE, "sql-statement was ==="+sqlbefehl_objekte+"=== municipality   id==="+evaluation.getmunicipalityDbId()+"===  name ==="+evaluation.getMunicipality()+"===");
				logger.log(Level.SEVERE, sqlerror.toString());
	//TODO stored errors, like org.postgresql.util.PSQLException: ERROR: GEOS covers() threw an error!
				System.out.println("ERROR: SQL-Exception during 1a. Action - get all closed way streets from osm, sql-statement was ==="+sqlbefehl_objekte+"=== municipality   id==="+evaluation.getmunicipalityDbId()+"===  name ==="+evaluation.getMunicipality()+"===");
				System.out.println(sqlerror.toString());
				try {
					if(polygonObjectsRS != null)
						polygonObjectsRS.close();
					if(polygonObjectsStmt != null)
						polygonObjectsStmt.close();
				} 
				catch(Exception e) {
					logger.log(Level.SEVERE, "General Exception, Details: " + e.toString());
					System.out.println("General Exception, Details: " + e.toString());
				}
			}
				
		} catch( SQLException sqlerror) {
			logger.log(Level.SEVERE, "ERROR: SQL-Exception during 1a. Action - get all closed way streets from osm, ...");
			logger.log(Level.SEVERE, "Query parameters ===" + statementParameters + "===");
			logger.log(Level.SEVERE, "sql-statement was ==="+sqlbefehl_objekte+"=== municipality   id==="+evaluation.getmunicipalityDbId()+"===  name ==="+evaluation.getMunicipality()+"===");
			logger.log(Level.SEVERE, sqlerror.toString());
//TODO stored errors, like org.postgresql.util.PSQLException: ERROR: GEOS covers() threw an error!
			System.out.println("ERROR: SQL-Exception during 1a. Action - get all closed way streets from osm, sql-statement was ==="+sqlbefehl_objekte+"=== municipality   id==="+evaluation.getmunicipalityDbId()+"===  name ==="+evaluation.getMunicipality()+"===");
			System.out.println(sqlerror.toString());
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
			sqlbefehl_objekte += " tags -> ? AS "+act_name+",";
		}
		sqlbefehl_objekte += " osm_id as id, tags->'place' as place, tags->'postal_code' AS postalcode,"
			+ " ST_AsText(ST_Transform(way,4326)) as point";
		if(examineStreetGeometryData) {
			sqlbefehl_objekte += ", ST_X(ST_Transform(way,4326)) AS leftbottom_xpos,"
				+ " ST_Y(ST_Transform(way,4326)) AS leftbottom_ypos,"
				+ " ST_X(ST_Transform(way,4326)) AS righttop_xpos,"
				+ " ST_Y(ST_Transform(way,4326)) AS righttop_ypos";
		}
		if(! akt_parameterstreetref.equals(""))
			sqlbefehl_objekte += ", tags -> ? AS streetref";
		else
			sqlbefehl_objekte += ", null AS streetref";
		sqlbefehl_objekte += " FROM planet_point";
		sqlbefehl_objekte += " WHERE ST_Covers(?::geometry, way) AND";
		sqlbefehl_objekte += " exist(tags, 'place')";
		for(Integer nameindex = 0; nameindex < keyvariation_namelist.length; nameindex++) {
			if(nameindex == 0)
				sqlbefehl_objekte += " AND (";
			sqlbefehl_objekte += " exist(tags, ?)";
			if(nameindex == (keyvariation_namelist.length -1))
				sqlbefehl_objekte += ")";
			else
				sqlbefehl_objekte += " OR";
		}
		sqlbefehl_objekte += " ORDER BY name;";

		PreparedStatement pointObjectsStmt = null;
		ResultSet pointObjectsRS = null;
		try {
			pointObjectsStmt = con_mapnik.prepareStatement(sqlbefehl_objekte);
			Integer statementIndex = 1;
			for(Integer nameindex = 0; nameindex < keyvariation_namelist.length; nameindex++) {
				String act_name = keyvariation_namelist[nameindex];
				pointObjectsStmt.setString(statementIndex++, act_name);
			}
			if(! akt_parameterstreetref.equals(""))
				pointObjectsStmt.setString(statementIndex++, akt_parameterstreetref);
			pointObjectsStmt.setString(statementIndex++, akt_gebietsgeometrie);
			for(Integer nameindex = 0; nameindex < keyvariation_namelist.length; nameindex++) {
				String act_name = keyvariation_namelist[nameindex];
				pointObjectsStmt.setString(statementIndex++, act_name);
			}
		
			Date local_query_start = new Date();

			pointObjectsRS = pointObjectsStmt.executeQuery();

			Date local_query_end = new Date();
			logger.log(Level.FINEST, "TIME single-step quer osm-points in ms. "+(local_query_end.getTime()-local_query_start.getTime()));
			pointsQueryDuration += local_query_end.getTime() - local_query_start.getTime();
			local_query_start = local_query_end;

			try {
				Date step_query_start = new Date();
				anzahl_datensaetze_osmstreets = 0;
				anzahl_datensaetze_osmplaces = 0;
				while( pointObjectsRS.next() ) {
					anzahl_datensaetze_osmplaces++;

						// get name of object
					String temp_aktstrasse = "";
					String act_name = "";
					for(Integer nameindex = 0; nameindex < keyvariation_namelist.length; nameindex++) {
						act_name = keyvariation_namelist[nameindex];
						if( pointObjectsRS.getString(act_name) != null) {
							temp_aktstrasse = pointObjectsRS.getString(act_name);
							if (!act_name.equals("name")) {
								logger.log(Level.FINER, "Ausgabe OSM-Straße Name-Variation (" + act_name + ") ==="+temp_aktstrasse +"===");
							}
							logger.log(Level.FINER, "OSM-Placename ==="+temp_aktstrasse+"===   place-value ==="+pointObjectsRS.getString("place")+"===   osm-id ==="+pointObjectsRS.getString("id")+"===");
							String temp_point = pointObjectsRS.getString("point");
							String temp_point_source = "";
							String local_osm_validkeyvalue = "";
								// check for all possible values, optionally separated by ;
							String[] local_keyvalue = pointObjectsRS.getString("place").split(";");
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
								String point_leftbottom = null;
								String point_righttop = null;
								if(examineStreetGeometryData) {
									point_leftbottom = "POINT(" + pointObjectsRS.getDouble("leftbottom_xpos") + " " + pointObjectsRS.getDouble("leftbottom_ypos") + ")";
									point_righttop = "POINT(" + pointObjectsRS.getDouble("righttop_xpos") + " " + pointObjectsRS.getDouble("righttop_ypos") + ")";
								}
								Street new_street =  new Street("osm", "node", local_osm_validkeyvalue, pointObjectsRS.getLong("id"), 
										temp_aktstrasse, pointObjectsRS.getString("streetref"), pointObjectsRS.getString("postalcode"), 
										temp_point, temp_point_source, point_leftbottom, point_righttop);
								if(streets.get(new_street) != null)
										streets.update(new_street);
								else
										streets.add(new_street);
							}
						}
					}
				}
				Date step_query_end = new Date();
				logger.log(Level.FINEST, "TIME single-step analyze osm-points in ms. "+(step_query_end.getTime()-step_query_start.getTime()));
				logger.log(Level.FINE, "planet_polygon - Number of streets: " + anzahl_datensaetze_osmstreets + "  Number of places: " + anzahl_datensaetze_osmplaces);
				pointsAnalyzeDuration += step_query_end.getTime() - step_query_start.getTime();
				pointObjectsRS.close();
				pointObjectsStmt.close();
			}
			catch( SQLException sqlerror) {
				logger.log(Level.SEVERE, "ERROR: SQL-Exception during 2. Action - get all point street objects from osm, ...");
				logger.log(Level.SEVERE, "Query parameters ===" + statementParameters + "===");
				logger.log(Level.SEVERE, "sql-statement was ==="+sqlbefehl_objekte+"=== municipality   id==="+evaluation.getmunicipalityDbId()+"===  name ==="+evaluation.getMunicipality()+"===");
				logger.log(Level.SEVERE, sqlerror.toString());
	//TODO stored errors, like org.postgresql.util.PSQLException: ERROR: GEOS covers() threw an error!
				System.out.println("ERROR: SQL-Exception during 2. Action - get all point street objects from osm, ...");
				System.out.println("Query parameters ===" + statementParameters + "===");
				System.out.println("sql-statement was ==="+sqlbefehl_objekte+"=== municipality   id==="+evaluation.getmunicipalityDbId()+"===  name ==="+evaluation.getMunicipality()+"===");
				System.out.println(sqlerror.toString());
				try {
					if(polygonObjectsRS != null)
						polygonObjectsRS.close();
					if(polygonObjectsStmt != null)
						polygonObjectsStmt.close();
				} 
				catch(Exception e) {
					logger.log(Level.SEVERE, "General Exception, Details: " + e.toString());
					System.out.println("General Exception, Details: " + e.toString());
				}
			}
		
		} catch( SQLException sqlerror) {
			logger.log(Level.SEVERE, "ERROR: SQL-Exception during 2. Action - get all point street objects from osm, ...");
			logger.log(Level.SEVERE, "Query parameters ===" + statementParameters + "===");
			logger.log(Level.SEVERE, "sql-statement was ==="+sqlbefehl_objekte+"=== municipality   id==="+evaluation.getmunicipalityDbId()+"===  name ==="+evaluation.getMunicipality()+"===");
			logger.log(Level.SEVERE, sqlerror.toString());
//TODO stored errors, like org.postgresql.util.PSQLException: ERROR: GEOS covers() threw an error!
			System.out.println("ERROR: SQL-Exception during 2. Action - get all point street objects from osm, ...");
			System.out.println("Query parameters ===" + statementParameters + "===");
			System.out.println("sql-statement was ==="+sqlbefehl_objekte+"=== municipality   id==="+evaluation.getmunicipalityDbId()+"===  name ==="+evaluation.getMunicipality()+"===");
			System.out.println(sqlerror.toString());
		}
			

		logger.log(Level.FINE, "   ----------------------------------------------------------------------------------------");
		logger.log(Level.FINE, "   ----------------------------------------------------------------------------------------");

		return streets;
	}
}
