

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;



public class StreetCollection {
	public HashMap<String,Street> cache = new HashMap<String,Street>(); 
	private EvaluationNew.FieldsForUniqueStreet fieldsForUniqueStreet = EvaluationNew.FieldsForUniqueStreet.STREET;
	private EvaluationNew.StreetNameIdenticalLevel streetnameidenticallevel = EvaluationNew.StreetNameIdenticalLevel.EXACTLY;

	
	public EvaluationNew.FieldsForUniqueStreet getFieldsForUniqueAddress() {
		return fieldsForUniqueStreet;
	}

	public void setFieldsForUniqueStreet(EvaluationNew.FieldsForUniqueStreet setfields) {
		fieldsForUniqueStreet = setfields;
	}

	public void setStreetnameidenticallevel(EvaluationNew.StreetNameIdenticalLevel setlevel) {
		streetnameidenticallevel = setlevel;
	}

	public void clear() {
		cache.clear();
		fieldsForUniqueStreet = EvaluationNew.FieldsForUniqueStreet.STREET;
	}

	public Integer size() {
		Integer count = 0;
    	for (Map.Entry<String,Street> entry : cache.entrySet()) {
    		count++;
    	}
		return count;
	}

	public String getNameForIdenticalcheck(String standardname) {
		if(streetnameidenticallevel == EvaluationNew.StreetNameIdenticalLevel.EXACTLY) {
			return standardname;
		} else {
			Charset iso88591charset = Charset.forName("ISO-8859-1");
			byte[] iso88591Data1 = standardname.getBytes(iso88591charset);
			String string1 = new String ( iso88591Data1, iso88591charset );
			string1 = utf8_to_chars26(string1).toUpperCase();
			string1 = string1.replaceAll(" +", " ");
			return string1;
		}
	}
	
	public String key(Street street) {
		String keystring = "";

		String streetname = getNameForIdenticalcheck(street.name);
		
		if(fieldsForUniqueStreet == EvaluationNew.FieldsForUniqueStreet.STREET) {
			keystring = streetname;
		} else if(fieldsForUniqueStreet == EvaluationNew.FieldsForUniqueStreet.STREET_REF) {
			keystring = streetname;
			if(street.streetref != null)
				keystring += street.streetref;
		} else if(fieldsForUniqueStreet == EvaluationNew.FieldsForUniqueStreet.STREET_POSTCODE) {
			keystring = streetname;
			if(street.postcode != null)
				keystring += street.postcode;
		} else if(fieldsForUniqueStreet == EvaluationNew.FieldsForUniqueStreet.STREET_POSTCODE_REF) {
			keystring = streetname;
			if(street.postcode != null)
				keystring += street.postcode;
			if(street.streetref != null)
				keystring += street.streetref;
		} else {
			keystring = null;
		}
		return keystring;
	}

	public Street get(Street searchstreet) {
			// check for key (although optionally with some normalization) could be not enough for lists like in Brasil
			// If smoother checks are necessary, look on old code in private repo in class StreetObject, method check_if_identical for earlier solution with levenshtein
		String key = this.key(searchstreet);
		if(cache.get(key) != null)
			return cache.get(key);
		else
			return null;
	}

	public void setEvaluationParameters(EvaluationNew evaluation) {
		this.setFieldsForUniqueStreet(evaluation.getfieldsForUniqueStreet());
		this.setStreetnameidenticallevel(evaluation.getStreetnameIdenticalLevel());
	}

	
	public boolean update(Street streettoupdate) {
		Street existingstreet = this.get(streettoupdate);
		if(existingstreet != null) {
			if(existingstreet.update(streettoupdate) != null)
				return true;
			else
				return false;
		} else {
			return false;
		}
	}

	public boolean add(Street newstreet) {
		if(this.get(newstreet) != null) {
			return false;
		}
		cache.put(this.key(newstreet),newstreet);
		return true;
	}

	public StreetCollection merge(StreetCollection otherstreetlist) {

		Integer strassen_soll_anzahl = 0;
		Integer anzahl_datensaetze_singlesoll = 0;
		Integer strassen_osm_single = 0;
		Integer anzahl_datensaetze_singleosm = 0;
		Integer strassen_ist_anzahl = 0;
		Integer strassen_osm_missing_local = 0;
		
    	for (Map.Entry<String,Street> entry : cache.entrySet()) {
			Street activestreet = entry.getValue();
			//if(housenumber.getstate().equals("unchanged"))
			//count++;

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
    	}		

		System.out.println("strassen_soll_anzahl: " + strassen_soll_anzahl);
		System.out.println("anzahl_datensaetze_singlesoll: " + anzahl_datensaetze_singlesoll);
		System.out.println("strassen_osm_single: " + strassen_osm_single);
		System.out.println("anzahl_datensaetze_singleosm: " + anzahl_datensaetze_singleosm);
		System.out.println("strassen_ist_anzahl: " + strassen_ist_anzahl);
		System.out.println("strassen_osm_missing_local: " + strassen_osm_missing_local);
    	
		return null;
	}


	private String utf8_to_chars26(String intext) {
		intext = intext.replace("1", "eins");
		intext = intext.replace("2", "zwei");
		intext = intext.replace("3", "DREI");
		intext = intext.replace("4", "vier");
		intext = intext.replace("5", "fuen");
		intext = intext.replace("6", "sech");
		intext = intext.replace("7", "sieb");
		intext = intext.replace("8", "acht");
		intext = intext.replace("9", "neun");
		intext = intext.replace("0", "null");

		intext = intext.replace("Â", "A");
		intext = intext.replace("Á", "A");
		intext = intext.replace("Ä", "A");
		intext = intext.replace("á", "a");
		intext = intext.replace("â", "a");
		intext = intext.replace("ã", "a");
		intext = intext.replace("ä", "a");
		intext = intext.replace("ç", "c");
		intext = intext.replace("é", "e");
		intext = intext.replace("ê", "e");
		intext = intext.replace("í", "i");
		intext = intext.replace("Ö", "O");
		intext = intext.replace("ö", "o");
		intext = intext.replace("ó", "o");
		intext = intext.replace("õ", "o");
		intext = intext.replace("ô", "o");
		intext = intext.replace("Ü", "U");
		intext = intext.replace("ü", "u");
		intext = intext.replace("ú", "u");
		return intext;
	}
}
