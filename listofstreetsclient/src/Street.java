


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
	protected String point_leftbottom;
	protected String point_righttop;
	

	private class Borderpoint {
		protected Double xpos;
		protected Double ypos;
	}

	private Borderpoint[] getBorderPositions(String leftbottomtext, String righttoptext) {
		Borderpoint return_leftbottom = new Borderpoint();
		Borderpoint return_righttop = new Borderpoint();
		Borderpoint[] returnarray = new Borderpoint[2];
		
		if((leftbottomtext == null) || leftbottomtext.equals("")) {
			return returnarray;
		}
				
		// bbox String has the form BOX(13.8382605689486 53.7045317757763,13.8392753957251 53.7047286850151)
		String searchtext = "(";
		Integer starttextpos = leftbottomtext.indexOf(searchtext);
		Integer endtextpos = -1;
		if(starttextpos != -1) {
			starttextpos += searchtext.length();
			searchtext = ")";
			endtextpos = leftbottomtext.indexOf(searchtext, starttextpos);
		}
		if((starttextpos == -1) || (endtextpos == -1)) {
			System.out.println("ERROR: can't find leftbuttom position in bbox ===" + leftbottomtext + "===");
			return null;
		} else {
			String leftbottom = leftbottomtext.substring(starttextpos, endtextpos);
			String[] leftbottom_parts = leftbottom.split(" ");
			if(leftbottom_parts.length == 2) {
				return_leftbottom.xpos = Double.parseDouble(leftbottom_parts[0]);
				return_leftbottom.ypos = Double.parseDouble(leftbottom_parts[1]);
			} else {
				System.out.println("ERROR: string for leftbuttom postion has invalid form ===" + leftbottomtext + "===");
				return null;
			}

			searchtext = "(";
			starttextpos = righttoptext.indexOf(searchtext);
			endtextpos = -1;
			if(starttextpos != -1) {
				starttextpos += searchtext.length();
				searchtext = ")";
				endtextpos = righttoptext.indexOf(searchtext, starttextpos);
			}
			if((starttextpos == -1) || (endtextpos == -1)) {
				System.out.println("ERROR: can't find righttop position in bbox ===" + righttoptext + "===");
				return null;
			} else {
				String righttop = righttoptext.substring(starttextpos, endtextpos);
				String[] righttop_parts = righttop.split(" ");
				if(righttop_parts.length == 2) {
					return_righttop.xpos = Double.parseDouble(righttop_parts[0]);
					return_righttop.ypos = Double.parseDouble(righttop_parts[1]);
				} else {
					System.out.println("ERROR: string for righttop postion has invalid form ===" + righttoptext + "===");
					return null;
				}
			}
		}
		returnarray[0] = return_leftbottom;
		returnarray[1] = return_righttop;
		return returnarray;
	}

	private String[] unionBoundingbox(Street newstreet) {
		String[] return_array = new String[2];

		if((this.point_leftbottom == null) || this.point_leftbottom.equals("")) {
			return return_array;
		}

		Borderpoint[] this_points = getBorderPositions(this.point_leftbottom, this.point_righttop);
		Borderpoint[] newstreet_points = getBorderPositions(newstreet.point_leftbottom, newstreet.point_righttop);

		if(		(this_points[0].xpos != 0.0D) && (this_points[0].ypos != 0.0D)
			&&	(this_points[1].xpos != 0.0D) && (this_points[1].ypos != 0.0D)
			&&	(newstreet_points[0].xpos != 0.0D) && (newstreet_points[0].ypos != 0.0D)
			&&	(newstreet_points[1].xpos != 0.0D) && (newstreet_points[1].xpos != 0.0D)) {

			if(newstreet_points[0].xpos < this_points[0].xpos)
				this_points[0].xpos = newstreet_points[0].xpos;
			if(newstreet_points[0].ypos < this_points[0].ypos)
				this_points[0].ypos = newstreet_points[0].ypos;

			if(newstreet_points[1].xpos > this_points[1].xpos)
				this_points[1].xpos = newstreet_points[1].xpos;
			if(newstreet_points[1].ypos > this_points[1].ypos)
				this_points[1].ypos = newstreet_points[1].ypos;
			return_array[0] = "POINT(" + this_points[0].xpos + " " + this_points[0].ypos + ")";
			return_array[1] = "POINT(" + this_points[1].xpos + " " + this_points[1].ypos + ")";
			return return_array;
		} else {
			return_array[0] = this.point_leftbottom;
			return_array[1] = this.point_righttop;
			return return_array;
		}
	}


	public Street(	String source, String in_osmtype, String in_osmobjectkeyvalue,
					Long in_id, String in_streetname, String in_streetref, String in_postcode, 
					String point, String point_source, String bbox_leftbottom, String bbox_righttop) {
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
		this.point_leftbottom = "";
		this.point_righttop = "";
		if((bbox_leftbottom != null) && !bbox_leftbottom.equals("")) {
			this.point_leftbottom = bbox_leftbottom;
		}
		if((bbox_righttop != null) && !bbox_righttop.equals("")) {
			this.point_righttop = bbox_righttop;
		}
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
				if(updatestreet.point_leftbottom != null) {
					String[] borderpointsastext = this.unionBoundingbox(updatestreet);
					this.point_leftbottom = borderpointsastext[0];
					this.point_righttop = borderpointsastext[1];
				}
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
