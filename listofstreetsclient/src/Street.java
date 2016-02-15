


import java.nio.charset.Charset;


public class Street {
	protected String source;
	protected String name;
	protected String streetref;
	protected String osm_id;									// can contains more than one id, separated by ,
	protected String osm_type;								// contains same number of entries, as in osm_id, separated by ,
	protected String osm_objectkeyvalue;
	protected String street_id;
	protected String postcode;
	protected String point;
	protected String point_source;
	protected String aktion;

	public Street(	String source, String in_osmtype, String in_osmobjectkeyvalue,
					Long in_id, String in_streetname, String in_streetref, String in_postcode, 
					String point, String point_source) {
//TODO first, check, if street already in map
		//System.out.println("Objekt ist komplett neu, also anhängen");
		this.source = source;
		if(source.equals("osm")) {
			this.osm_id = "" + in_id;
			this.osm_type = in_osmtype;
			this.osm_objectkeyvalue = in_osmobjectkeyvalue;
			this.street_id = "";
		} else {
			this.street_id = "" + in_id;
			this.osm_id = "";
		}
		this.name = in_streetname;
		this.streetref = in_streetref;
		this.postcode = in_postcode;
		this.point = point;
		this.point_source = point_source;
		this.aktion = "neu";
	}


	public Street update(	Street updatestreet) {

//TODO first, check, if identically key???

		boolean neues_objekt_schon_vorhanden = true;

		if(updatestreet.source.equals("osm")) {
			if(! this.source.equals("osm")) {
				this.osm_id = "" + updatestreet.osm_id;
				this.osm_type = updatestreet.osm_type;
				this.osm_objectkeyvalue = updatestreet.osm_objectkeyvalue;
			} else {
				this.osm_id += "," + "" + updatestreet.osm_id;
				this.osm_type += "," + updatestreet.osm_type;
				this.osm_objectkeyvalue += "," + updatestreet.osm_objectkeyvalue;
			}
		} else {
			if(this.street_id.equals(""))
				this.street_id = "" + updatestreet.street_id;
			else
				this.street_id += "," + "" + updatestreet.street_id;
		}
			// if point has value
		if( ! point.equals("")) {
				// and source=osm then store point or if source=street and existing point is not from osm then store point
			if((updatestreet.source.equals("osm")) ||
				 (updatestreet.source.equals("street") && !this.source.equals("osm"))) {
				this.point = updatestreet.point;
				this.point_source = updatestreet.point_source;
			}
		}
		this.aktion = "aktualisiert";
		return this;

/*		if( ! neues_objekt_schon_vorhanden ) {
			StreetObject dummy_neu = new StreetObject();
			dummy_neu.aktion = "fehlt";
			return dummy_neu;
		}
		return new StreetObject();
*/
		}
	}
