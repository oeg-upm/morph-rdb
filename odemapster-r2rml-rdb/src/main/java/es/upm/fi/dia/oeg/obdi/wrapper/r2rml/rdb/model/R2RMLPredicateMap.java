package es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model;

import com.hp.hpl.jena.rdf.model.Resource;

import es.upm.fi.dia.oeg.morph.base.Constants;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.exception.R2RMLInvalidTermMapException;

public class R2RMLPredicateMap extends R2RMLTermMap {
	
	public R2RMLPredicateMap(String constantValue) {
		super(TermMapPosition.PREDICATE, constantValue);
		super.setTermType(Constants.R2RML_LITERAL_URI());
	}
	
	public R2RMLPredicateMap(Resource resource, R2RMLTriplesMap owner) throws R2RMLInvalidTermMapException {
		super(resource, TermMapPosition.PREDICATE, owner);
	}

}
