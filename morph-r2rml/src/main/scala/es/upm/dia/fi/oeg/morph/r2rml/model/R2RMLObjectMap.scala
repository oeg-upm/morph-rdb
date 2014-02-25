package es.upm.dia.fi.oeg.morph.r2rml.model

import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping
import com.hp.hpl.jena.rdf.model.Resource
import es.upm.fi.dia.oeg.morph.base.Constants

class R2RMLObjectMap extends R2RMLTermMap {
	override val logger = Logger.getLogger(this.getClass().getName());

	def this(resource:Resource) = {
		this();
		this.parse(resource);
	}
	
	def this(constantValue:String ) =  {
		this();
		this.termMapType = Constants.MorphTermMapType.ConstantTermMap;
		this.constantValue = constantValue;
		this.termType = this.getDefaultTermType();
	}	
	
}

