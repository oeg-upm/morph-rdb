package es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model;

import com.hp.hpl.jena.rdf.model.Resource;

import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.exception.R2RMLInvalidTermMapException;

public class R2RMLObjectMap extends R2RMLTermMap {
	
	public R2RMLObjectMap(Resource resource, R2RMLTriplesMap owner) throws R2RMLInvalidTermMapException {
		super(resource, TermMapPosition.OBJECT, owner);
	}
	
	public R2RMLObjectMap(String constantValue) {
		super(TermMapPosition.OBJECT, constantValue);		
	}
}
