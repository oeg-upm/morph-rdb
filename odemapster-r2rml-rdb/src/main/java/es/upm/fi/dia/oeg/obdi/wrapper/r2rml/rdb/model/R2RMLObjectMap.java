package es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model;

import com.hp.hpl.jena.rdf.model.Resource;

import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.exception.R2RMLInvalidTermMapException;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTermMap.TermMapType;

public class R2RMLObjectMap extends R2RMLTermMap {
	
	public static R2RMLObjectMap create(Resource resource, R2RMLTriplesMap owner) 
			throws R2RMLInvalidTermMapException {
		R2RMLObjectMap om = new R2RMLObjectMap();
		om.parse(resource, owner);
		return om;
	}
	
//	public R2RMLObjectMap(String constantValue) {
//		super(TermMapPosition.OBJECT, constantValue);		
//	}
	
	public static R2RMLObjectMap create(String constantValue) {
		R2RMLObjectMap om = new R2RMLObjectMap();
		om.termMapType = TermMapType.CONSTANT;
		om.constantValue = constantValue;
		return om;
	}	
}
