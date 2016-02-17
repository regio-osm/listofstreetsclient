

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
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



/**
 * @author Dietmar Seifert
 *
 * evaluate the housenumbers in a municipality or a subregion of it, check againsts an available housenumerlist
 * 	
*/




public class StreetlistReader {
	Applicationconfiguration configuration = new Applicationconfiguration();
	private static Logger logger = (Logger) EvaluationNew.logger;

	private StreetCollection streets = new StreetCollection();

	static Connection con_listofstreets = null;


	public void setExistingStreetlist(StreetCollection streets) {
		this.streets = streets;
	}

	public StreetCollection ReadListFromDB(EvaluationNew evaluation) {

		if(		(evaluation.getCountry().equals(""))
			||	(evaluation.getMunicipality().equals(""))) {
				return null;
			}		

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


		try {

				//Connection of own project-specific DB
			String url_listofstreets = configuration.db_application_url;
			con_listofstreets = DriverManager.getConnection(url_listofstreets, configuration.db_application_username, configuration.db_application_password);
		}
		catch (Exception e) {
			logger.log(Level.INFO, "ERROR: failed to connect to DB===");
			logger.log(Level.INFO, e.toString());
			System.out.println("ERROR: failed to connect to DB===");
			System.out.println(e.toString());
			return streets;
		}

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
	


			
		Integer anzahl_datensaetze_osmstreets = 0;
		Integer anzahl_datensaetze_osmplaces = 0;
		Integer anzahl_datensaetze_sollliste = 0;
		Integer anzahl_datensaetze_singlesoll = 0;
		Integer anzahl_datensaetze_singleosm = 0;

		String akt_jobname = evaluation.getMunicipality();
		String akt_parameterstreetref = "";
		if(evaluation.isStreetrefMustBeUsedForIdenticaly()) {
			akt_parameterstreetref = evaluation.getStreetrefOSMKey();
		}

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

			
		try {
			// ------------------------------------------------------------------------------
			// 3. Action - get all street from street list
			// eigentlich als extra Lauf überflüssig, weil weiter unten die nur-Soll-Liste eigenständig erstellt wird
		//produktiv bis 03.03.2013, jetzt werden die Spalten point_state und point_source nicht mehr gesetzt, weil Tabelle evalation_street die Daten haelt - sqlbefehl_objekte = "SELECT DISTINCT ON (name) name, id, ST_AsText(point) AS point, point_state, point_source ";
			sqlbefehl_objekte = "SELECT DISTINCT ON (name, streetref) name, id, ST_AsText(point) AS point, point_source, streetref,";
//TODO add column postalcode in table street and fill during streetlist import
//TODO And fill streetref column during streetlist import, too
sqlbefehl_objekte += " null as postalcode";
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
				String temp_linecenterpoint = "";
				String temp_point_source = "";
				if( ! temp_linecenterpoint.equals(""))
					temp_point_source = "OSM";
				Street new_street =  new Street("street", "", "", rs_objekte.getLong("id"), rs_objekte.getString("name"),
					rs_objekte.getString("streetref"), rs_objekte.getString("postalcode"), temp_linecenterpoint, temp_point_source, "", "");
				if(streets.get(new_street) != null)
						streets.update(new_street);
				else
						streets.add(new_street);
			}
			rs_objekte.close();
			stmt_objekte.close();
			con_listofstreets.close();
		}
		catch( SQLException sqle) {
			logger.log(Level.INFO, sqle.toString());
			System.out.println(sqle.toString());
			try {
				if(rs_objekte != null)
					rs_objekte.close();
				if(stmt_objekte != null)
					stmt_objekte.close();
				if(con_listofstreets != null)
					con_listofstreets.close();
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
