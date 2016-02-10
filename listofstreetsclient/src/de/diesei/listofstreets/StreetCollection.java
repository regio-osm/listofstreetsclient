package de.diesei.listofstreets;


import java.util.TreeMap;



public class StreetCollection {
	public TreeMap<String,Street> cache = new TreeMap<String,Street>(); 
	private FieldsForUniqueStreet fieldsForUniqueStreet = FieldsForUniqueStreet.STREET;
	private StreetNameIdenticalLevel streetnameidenticallevel = StreetNameIdenticalLevel.EXACTLY;

	public enum FieldsForUniqueStreet {
		STREET, STREET_REF, STREET_POSTCODE, STREET_POSTCODE_REF;
	}
	
	public enum StreetNameIdenticalLevel {
		ROUGHLY, EXACTLY;
	}
	
	public FieldsForUniqueStreet getFieldsForUniqueAddress() {
		return fieldsForUniqueStreet;
	}

	public void setFieldsForUniqueAddress(FieldsForUniqueStreet setfields) {
		fieldsForUniqueStreet = setfields;
	}

	public void clear() {
		cache.clear();
		fieldsForUniqueStreet = FieldsForUniqueStreet.STREET;
	}

}
