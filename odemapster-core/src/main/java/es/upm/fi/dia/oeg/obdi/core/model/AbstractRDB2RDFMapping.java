package es.upm.fi.dia.oeg.obdi.core.model;



public abstract class AbstractRDB2RDFMapping {
	public enum MappingType {
		CONCEPT, ATTRIBUTE, RELATION
	}
	
	protected String name;
	protected String documentation;
	protected String id;
	
	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public String getId() {
		if(this.id != null && !this.id.equals("")) {
			return this.id;
		} else {
			return this.hashCode() + "";
		}
	}
	
	
}

