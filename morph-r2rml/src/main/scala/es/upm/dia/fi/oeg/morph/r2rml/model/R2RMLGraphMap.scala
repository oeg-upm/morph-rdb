package es.upm.dia.fi.oeg.morph.r2rml.model

import scala.collection.JavaConversions._

import es.upm.fi.dia.oeg.morph.base.ConfigurationProperties
import com.hp.hpl.jena.rdf.model.Resource
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping

class R2RMLGraphMap 
extends R2RMLTermMap {
	

		
	def this(resource:Resource) = {
	  this();
	  this.parse(resource);
	}
	
	def this(constantValue:String ) = {
		this();
		this.termMapType = Constants.MorphTermMapType.ConstantTermMap;
		this.constantValue = constantValue;
		this.termType = Constants.R2RML_LITERAL_URI;
	}

}

object R2RMLGraphMap {
	
	def extractGraphMaps(resource:Resource) : Set[R2RMLGraphMap]= {
		val graphMapStatements = resource.listProperties(Constants.R2RML_GRAPHMAP_PROPERTY);
		val graphMaps1 = if(graphMapStatements != null) {
		  graphMapStatements.toList().flatMap(graphMapStatement => {
				if(graphMapStatement != null) {
					val graphMapResource = graphMapStatement.getObject().asInstanceOf[Resource];
					val gm = new R2RMLGraphMap(graphMapResource);
					gm.parse(graphMapResource);
					if(gm.termType != null && !gm.termType.equals(Constants.R2RML_IRI_URI)) {
						throw new Exception("Non IRI value is not permitted in the graph!");
					}
					Some(gm);
				} else {
				  None
				}		    
		  })
		} else {
		  Set.empty
		}

		val graphStatements = resource.listProperties(Constants.R2RML_GRAPH_PROPERTY);
		val graphMaps2 = if(graphStatements != null) {
		  graphStatements.toList().flatMap(graphStatement => {
					val graphStatementObjectValue = graphStatement.getObject().toString();
					if(!Constants.R2RML_DEFAULT_GRAPH_URI.equals(graphStatementObjectValue)) {
						val gm = new R2RMLGraphMap(graphStatementObjectValue);
						Some(gm);
					} else {
					  None
					}
		  })

		} else {
		  Set.empty
		}
		
		val graphMaps = graphMaps1.toList ::: graphMaps2.toList;
		val graphMapsInSet = graphMaps.toSet;
		graphMapsInSet;
	}	
}