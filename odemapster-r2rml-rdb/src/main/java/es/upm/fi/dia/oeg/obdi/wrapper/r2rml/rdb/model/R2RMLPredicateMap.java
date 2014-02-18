package es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model;

import com.hp.hpl.jena.rdf.model.Resource;

import es.upm.fi.dia.oeg.morph.base.Constants;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.exception.R2RMLInvalidTermMapException;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTermMap.TermMapType;

public class R2RMLPredicateMap extends R2RMLTermMap {
	
//	public R2RMLPredicateMap(String constantValue) {
//		super(TermMapPosition.PREDICATE, constantValue);
//		super.setTermType(Constants.R2RML_LITERAL_URI());
//	}
	
	public static R2RMLPredicateMap create(Resource resource, R2RMLTriplesMap owner) 
			throws R2RMLInvalidTermMapException {
		R2RMLPredicateMap pm = new R2RMLPredicateMap();
		pm.parse(resource, owner);
		return pm;
	}

	public static R2RMLPredicateMap create(String constantValue) {
		R2RMLPredicateMap pm = new R2RMLPredicateMap();
		pm.termMapType = TermMapType.CONSTANT;
		pm.constantValue = constantValue;
		pm.termType = Constants.R2RML_IRI_URI();
		return pm;
	}	
}
