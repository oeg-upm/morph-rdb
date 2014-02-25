package es.upm.dia.fi.oeg.morph.r2rml.model

import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.Constants
import com.hp.hpl.jena.rdf.model.Resource
import java.util.HashSet
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping

class R2RMLSubjectMap(classURIs:Set[String], graphMaps:Set[R2RMLGraphMap]) extends R2RMLTermMap {
	override val logger = Logger.getLogger(this.getClass().getName());
//	var classURIs:Set[String] = Set.empty;
//	var graphMap:R2RMLGraphMap = null;

  	def this(constantValue:String) = {
		this(Set.empty, Set.empty);
		this.termMapType = Constants.MorphTermMapType.ConstantTermMap;
		this.constantValue = constantValue;
		this.termType = Constants.R2RML_IRI_URI;
	}
	

	
	def getClassURIs() : Set[String] = { this.classURIs; }

	def getGraphMaps() : Set[R2RMLGraphMap] = { this.graphMaps }

}

object R2RMLSubjectMap {
	val logger = Logger.getLogger(this.getClass().getName());

	def apply(resource:Resource) : R2RMLSubjectMap = {
		val classStatements = resource.listProperties(Constants.R2RML_CLASS_PROPERTY);
		val classURIs :Set[String] = if(classStatements != null) {
			classStatements.map(classStatement => {
			  classStatement.getObject().toString();
			}).toSet;
		} else {
		  Set.empty
		}
		
		val graphMaps = R2RMLGraphMap.extractGraphMaps(resource);
		
		val sm = new R2RMLSubjectMap(classURIs, graphMaps);
		sm.parse(resource);
		if(sm.termType != null && sm.termType.equals(Constants.R2RML_LITERAL_URI)) {
			throw new Exception("Literal is not permitted in the subject!");
		}
		
		return sm;
	}
	
	def extractSubjectMaps(resource:Resource) : Set[R2RMLSubjectMap]= {
		val subjectMapStatements = resource.listProperties(Constants.R2RML_SUBJECTMAP_PROPERTY);
		val subjectMaps1 = if(subjectMapStatements != null) {
		  subjectMapStatements.toList().flatMap(subjectMapStatement => {
				if(subjectMapStatement != null) {
					val subjectMapResource = subjectMapStatement.getObject().asInstanceOf[Resource];
					val sm = R2RMLSubjectMap(subjectMapResource);
					Some(sm);
				} else {
				  None
				}		    
		  })
		} else {
		  Set.empty
		}

		val sStatements = resource.listProperties(Constants.R2RML_SUBJECT_PROPERTY);
		val subjectMaps2 = if(sStatements != null) {
			sStatements.toList().flatMap(subjectStatement => {
				if(subjectStatement != null) {
					val constantValueObject = subjectStatement.getObject().toString();
					val subjectMap = new R2RMLSubjectMap(constantValueObject);
					Some(subjectMap);
				} else {
				  None
				}			  
			}); 
		} else {
		  Nil
		}
		
		val subjectMaps = subjectMaps1.toSet ++ subjectMaps2.toSet;
		subjectMaps;
	}		

}