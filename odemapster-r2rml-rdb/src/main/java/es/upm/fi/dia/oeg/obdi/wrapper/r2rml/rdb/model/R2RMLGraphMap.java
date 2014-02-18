package es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model;

import com.hp.hpl.jena.rdf.model.Resource;

import es.upm.fi.dia.oeg.morph.base.Constants;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.exception.R2RMLInvalidTermMapException;

public class R2RMLGraphMap extends R2RMLTermMap {
	
	public static R2RMLGraphMap create(Resource resource, R2RMLTriplesMap owner) throws R2RMLInvalidTermMapException {
		R2RMLGraphMap gm = new R2RMLGraphMap();
		gm.parse(resource, owner);
		return gm;
	}
	
	public static R2RMLGraphMap create(String constantValue) {
		R2RMLGraphMap gm = new R2RMLGraphMap();
		gm.termMapType = TermMapType.CONSTANT;
		gm.constantValue = constantValue;
		gm.termType = Constants.R2RML_IRI_URI();
		return gm;
	}
	
	


}
