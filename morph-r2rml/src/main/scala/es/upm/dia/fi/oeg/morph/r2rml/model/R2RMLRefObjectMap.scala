package es.upm.dia.fi.oeg.morph.r2rml.model

import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLJoinCondition
import com.hp.hpl.jena.rdf.model.RDFNode
import com.hp.hpl.jena.rdf.model.Resource
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.obdi.core.model.AbstractLogicalTable
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping

class R2RMLRefObjectMap(val parentTriplesMapResource:Resource, val joinConditions:Set[R2RMLJoinCondition]) {
//	var owner:AbstractMappingDocument=null ;
//	var resource:Resource=null;
	
	val logger = Logger.getLogger(this.getClass().getName());
	
	//private String alias;
	
	

//	public String getAlias() {
//		return alias;
//	}

	def getJoinConditions() : java.util.Collection[R2RMLJoinCondition] = {
		joinConditions;
	}

//	def getParentDatabaseColumnsString() : java.util.List[String] = {
//		val parentMap = this.getParentTriplesMap();
//		val result = parentMap.getSubjectReferencedColumns();
//		result;
//	}
//	
//	def getParentLogicalTable() : AbstractLogicalTable  = {
//		val result = try {
//			val triplesMap = this.getParentTriplesMap();
//			if(triplesMap != null) {
//				triplesMap.getLogicalTable();
//			}	else {
//			  null
//			}		
//		} catch {
//		  case e:Exception => {
//			val errorMessage = "Error while getting parent logical table!";
//			logger.warn(errorMessage);
//			null		    
//		  }
//		}
//
//		result;
//	}

	def getParentTripleMapName() :  String = {
		this.parentTriplesMapResource.getURI();
	}

//	def getParentTriplesMap() : AbstractConceptMapping  = {
//		val parentTriplesMapKey = this.parentTriplesMap.asResource().getLocalName();
//		val triplesMap = this.owner.getConceptMappingByMappingId(parentTriplesMapKey);
//		triplesMap;
//	}

	override def toString() : String = {
		this.parentTriplesMapResource.toString();
	}


}

object R2RMLRefObjectMap {
	val logger = Logger.getLogger(this.getClass().getName());
	
	def apply(resource:Resource) : R2RMLRefObjectMap = {
		val parentTriplesMapStatement = resource.getProperty(
		    Constants.R2RML_PARENTTRIPLESMAP_PROPERTY);
		
		val parentTriplesMap = if(parentTriplesMapStatement != null)  {
			parentTriplesMapStatement.getObject().asInstanceOf[Resource];
		} else {
		  null
		}
		
		val joinConditionsStatements = resource.listProperties(
		    Constants.R2RML_JOINCONDITION_PROPERTY);
		val joinConditions:Set[R2RMLJoinCondition] = if(joinConditionsStatements != null) {
		  joinConditionsStatements.map(joinConditionStatement => {
				val joinConditionResource = joinConditionStatement.getObject().asInstanceOf[Resource];
				val joinCondition = new R2RMLJoinCondition(joinConditionResource); 
				joinCondition
		  }).toSet
		} else {
			val errorMessage = "No join conditions defined!";
			logger.warn(errorMessage);
			Set.empty;
		}
		
		val rom = new R2RMLRefObjectMap(parentTriplesMap, joinConditions);
//		rom.resource = resource;
		rom
	}
	
	def isRefObjectMap(resource:Resource ) : Boolean  = {
		val parentTriplesMapStatement = resource.getProperty(
		    Constants.R2RML_PARENTTRIPLESMAP_PROPERTY);
		val hasParentTriplesMap = if(parentTriplesMapStatement != null)  {
			true;
		} else {
		  false
		}
		hasParentTriplesMap;
	}
	
  
}