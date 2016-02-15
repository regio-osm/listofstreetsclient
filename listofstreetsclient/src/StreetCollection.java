

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

	public String key(Street street) {
		String keystring = "";

		if(fieldsForUniqueStreet == EvaluationNew.FieldsForUniqueStreet.STREET) {
			keystring = street.name;
		} else if(fieldsForUniqueStreet == EvaluationNew.FieldsForUniqueStreet.STREET_REF) {
			keystring = street.name;
			if(street.streetref != null)
				keystring += street.streetref;
		} else if(fieldsForUniqueStreet == EvaluationNew.FieldsForUniqueStreet.STREET_POSTCODE) {
			keystring = street.name;
			if(street.postcode != null)
				keystring += street.postcode;
		} else if(fieldsForUniqueStreet == EvaluationNew.FieldsForUniqueStreet.STREET_POSTCODE_REF) {
			keystring = street.name;
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
}
