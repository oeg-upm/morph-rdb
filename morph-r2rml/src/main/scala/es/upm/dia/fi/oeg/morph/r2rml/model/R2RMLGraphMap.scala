package es.upm.dia.fi.oeg.morph.r2rml.model

import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTermMap
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTermMap.TermMapPosition
import es.upm.fi.dia.oeg.morph.base.ConfigurationProperties
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTriplesMap
import com.hp.hpl.jena.rdf.model.Resource
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTermMap.TermMapType
import es.upm.fi.dia.oeg.morph.base.Constants

class R2RMLGraphMap extends R2RMLTermMap {

}

object R2RMLGraphMap {
	def create(resource:Resource , owner:R2RMLTriplesMap ) : R2RMLGraphMap = {
		val gm = new R2RMLGraphMap();
		gm.parse(resource, owner);
		gm;
	}
	
	def create(constantValue:String ) : R2RMLGraphMap  = {
		val gm = new R2RMLGraphMap();
		gm.setTermMapType(TermMapType.CONSTANT);
		gm.setConstantValue(constantValue);
		gm.setTermType(Constants.R2RML_LITERAL_URI);
		return gm;
	}  
}