package es.upm.dia.fi.oeg.morph.r2rml.model

import scala.collection.JavaConversions._

import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping
import com.hp.hpl.jena.rdf.model.Resource

class R2RMLPredicateMap() extends R2RMLTermMap {
	
	def this(resource:Resource) = {
		this();
		this.parse(resource);
		this.resource = resource;
	}

	def this(constantValue:String) = {
		this();
		this.termMapType = Constants.MorphTermMapType.ConstantTermMap;
		this.constantValue = constantValue;
		this.termType = Constants.R2RML_IRI_URI;
	}	  
}

object R2RMLPredicateMap {
	def extractPredicateMaps(resource:Resource) 
	: List[R2RMLPredicateMap] = {
		val pmStatements = resource.listProperties(Constants.R2RML_PREDICATEMAP_PROPERTY);
		val predicateMaps1 = if(pmStatements != null) {
			pmStatements.toList().flatMap(pmStatement => {
				if(pmStatement != null) {
					val predicateMapResource = pmStatement.getObject().asInstanceOf[Resource];
					val pm = new R2RMLPredicateMap(predicateMapResource);
					Some(pm);
				} else {
				  None
				}			  
			});
		} else {
		  Nil
		}
		
		val pStatements = resource.listProperties(Constants.R2RML_PREDICATE_PROPERTY);
		val predicateMaps2 = if(pStatements != null) {
			pStatements.toList().flatMap(predicateStatement => {
				if(predicateStatement != null) {
					val constantValueObject = predicateStatement.getObject().toString();
					val predicateMap = new R2RMLPredicateMap(constantValueObject);
					Some(predicateMap);
				} else {
				  None
				}			  
			}); 
		} else {
		  Nil
		}

		val predicateMaps = predicateMaps1.toList ::: predicateMaps2.toList;
		predicateMaps
	}  
}