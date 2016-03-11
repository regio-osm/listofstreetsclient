

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import de.regioosm.listofstreetsclient.Applicationconfiguration;



/**
 * @author Dietmar Seifert
 *
 * evaluate the housenumbers in a municipality or a subregion of it, check againsts an available housenumerlist
 * 	
*/




public class StreetlistReader {
	static Applicationconfiguration configuration = new Applicationconfiguration();
	private static Logger logger = (Logger) EvaluationNew.logger;
	static Connection con_listofstreets = null;


	private StreetCollection streets = new StreetCollection();

	public StreetlistReader() {
		String url_listofstreets = ""; 

		this.initialize();

		if(con_listofstreets == null) {
			try {
	
					//Connection of own project-specific DB
				url_listofstreets = configuration.db_application_url;
				con_listofstreets = DriverManager.getConnection(url_listofstreets, configuration.db_application_username, configuration.db_application_password);
			}
			catch (Exception e) {
				logger.log(Level.INFO, "ERROR: failed to connect to DB ===" + url_listofstreets + "===");
				logger.log(Level.INFO, e.toString());
				System.out.println("ERROR: failed to connect to DB ===" + url_listofstreets + "===");
				System.out.println(e.toString());
				return;
			}
		}
	}

	/**
	 * initial class properties
	 */
	public void initialize() {
		streets.clear();
	}
	
	
	/**
	 * 
	 */
	public void close() {
			try {
				if(con_listofstreets != null) {
					logger.log(Level.INFO, "Connection to listofstreets DB " + con_listofstreets.getMetaData().getURL() + " will be closed.");
					con_listofstreets.close();
				}
			}
			catch( SQLException sqle) {
				logger.log(Level.SEVERE, "SQL-Exception occured, when tried to close DB Connection, details follow ..");
				logger.log(Level.SEVERE, sqle.toString());
				System.out.println("SQL-Exception occured, when tried to close DB Connection, details follow ..");
				System.out.println(sqle.toString());
			}
	}


	public void setExistingStreetlist(StreetCollection streets) {
		this.streets = streets;
	}

	
	public StreetCollection ReadListFromDB(EvaluationNew evaluation) {

		if(		(evaluation.getCountry().equals(""))
			||	(evaluation.getMunicipality().equals(""))) {
			return new StreetCollection();
		}		

		if(con_listofstreets == null) {
			return new StreetCollection();
		}



		String akt_jobname = evaluation.getMunicipality();

		logger.log(Level.FINE, "   ----------------------------------------------------------------------------------------");
		logger.log(Level.FINE, "   ----------------------------------------------------------------------------------------");
		logger.log(Level.FINE, "   Read List Data - Job  =" + akt_jobname +"=   muni-id: "+evaluation.getmunicipalityDbId());


		Long akt_municipality_id = evaluation.getmunicipalityDbId();

		String sqlbefehl_objekte = "";
		PreparedStatement stmt_objekte = null;
		ResultSet rs_objekte = null;


			
		try {
			// ------------------------------------------------------------------------------
			// 3. Action - get all street from street list
			// eigentlich als extra Lauf überflüssig, weil weiter unten die nur-Soll-Liste eigenständig erstellt wird
		//produktiv bis 03.03.2013, jetzt werden die Spalten point_state und point_source nicht mehr gesetzt, weil Tabelle evalation_street die Daten haelt - sqlbefehl_objekte = "SELECT DISTINCT ON (name) name, id, ST_AsText(point) AS point, point_state, point_source ";

			sqlbefehl_objekte = "SELECT DISTINCT ON (name, streetref) name, id, ST_AsText(point) AS point, point_source, streetref,"
			//TODO add column postalcode in table street and fill during streetlist import
			//TODO And fill streetref column during streetlist import, too
				+ " null as postalcode"
				+ " FROM street"
				+ " WHERE municipality_id = ?"
				+ " ORDER BY name;";

			logger.log(Level.FINEST, "sqlbefehl_objekte ==="+sqlbefehl_objekte+"===");
			stmt_objekte = con_listofstreets.prepareStatement(sqlbefehl_objekte);
			stmt_objekte.setLong(1, akt_municipality_id);
			rs_objekte = stmt_objekte.executeQuery( );
			while( rs_objekte.next() ) {
				String temp_aktstrasse = rs_objekte.getString("name");
				logger.log(Level.FINER, "Soll-Straße ==="+temp_aktstrasse +"===    point_source==="+rs_objekte.getString("point_source")+"===");
				String temp_linecenterpoint = "";
				String temp_point_source = "";
				if( ! temp_linecenterpoint.equals(""))
					temp_point_source = "OSM";
				Street new_street =  new Street("street", "", "", rs_objekte.getLong("id"), rs_objekte.getString("name"),
					rs_objekte.getString("streetref"), rs_objekte.getString("postalcode"), temp_linecenterpoint, temp_point_source, "", "");
				if((streets != null) && (streets.get(new_street) != null)) {
					streets.update(new_street);
				} else {
					streets.add(new_street);
				}
			}
			rs_objekte.close();
			stmt_objekte.close();
			//con_listofstreets.close();

			logger.log(Level.FINE, "   ----------------------------------------------------------------------------------------");
			logger.log(Level.FINE, "   ----------------------------------------------------------------------------------------");
		}
		catch( SQLException sqle) {
			logger.log(Level.SEVERE, "SQL-Exception, Details ===" + sqle.toString());
			System.out.println(sqle.toString());
			try {
				if(rs_objekte != null)
					rs_objekte.close();
				if(stmt_objekte != null)
					stmt_objekte.close();
				//if(con_listofstreets != null)
				//	con_listofstreets.close();
				return null;
			} 
			catch(Exception e) {
				logger.log(Level.SEVERE, "General Exception, Details: " + e.toString());
				System.out.println("General Exception, Details: " + e.toString());
			}
			return null;
		}
		return streets;
	}
	
}
