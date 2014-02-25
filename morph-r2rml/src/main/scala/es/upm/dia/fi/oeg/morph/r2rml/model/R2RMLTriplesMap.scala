package es.upm.dia.fi.oeg.morph.r2rml.model

import scala.collection.JavaConversions._
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.engine.R2RMLElement
import es.upm.fi.dia.oeg.obdi.core.model.IConceptMapping
import com.hp.hpl.jena.rdf.model.Resource
import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLLogicalTable
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.engine.R2RMLElementVisitor
import es.upm.fi.dia.oeg.obdi.core.model.AbstractPropertyMapping
import es.upm.fi.dia.oeg.obdi.core.model.AbstractRDB2RDFMapping.MappingType
import es.upm.fi.dia.oeg.obdi.core.model.IRelationMapping
import es.upm.fi.dia.oeg.obdi.core.model.AbstractLogicalTable
import java.sql.DatabaseMetaData
import es.upm.fi.dia.oeg.morph.base.sql.MorphDatabaseMetaData
import es.upm.fi.dia.oeg.morph.base.sql.MorphTableMetaData

class R2RMLTriplesMap(val logicalTable:R2RMLLogicalTable, val subjectMap:R2RMLSubjectMap
    , val predicateObjectMaps:Set[R2RMLPredicateObjectMap]) 
extends AbstractConceptMapping with R2RMLElement with IConceptMapping
{
  	val logger = Logger.getLogger(this.getClass().getName());
	var triplesMapName:String = null;
	
	def buildMetaData(dbMetadata:MorphDatabaseMetaData) = {
	  this.logicalTable.buildMetaData(dbMetadata);
	  this.subjectMap.buildMetadata(dbMetadata);
	  this.predicateObjectMaps.foreach(pom => pom.buildMetadata(dbMetadata));
	}
	
	def accept(visitor:R2RMLElementVisitor ) : Object ={ return visitor.visit(this); }
	
	override def toString() : String = { return this.triplesMapName; }
	
	override def getConceptName() :String = {
		var result : String = null;
		
		val classURIs = this.subjectMap.getClassURIs();
		if(classURIs == null || classURIs.size() == 0) {
			logger.warn("No class URI defined for TriplesMap: " + this);
		} else {
			if(classURIs.size() > 1) {
				logger.warn("Multiple classURIs defined, only one is returned!");
			}
			result = classURIs.iterator().next();
		}

		return result;
	}	
	
	override def getPropertyMappings(propertyURI:String ) 
	: java.util.Collection[AbstractPropertyMapping] = {
		val poms= this.predicateObjectMaps;
		val result = poms.filter(pom => {
				val predicateMapValue = pom.getPredicateMap(0).getOriginalValue();
				predicateMapValue.equals(propertyURI)
			})
		
		result;
	}
	
	override def getPropertyMappings() : java.util.Collection[AbstractPropertyMapping] = {
	  this.predicateObjectMaps
	}
	
	override def getRelationMappings() : java.util.Collection[IRelationMapping] = {
		val result = if(this.predicateObjectMaps != null) {
			this.predicateObjectMaps.flatMap(pm => {
				val mappingType = pm.getPropertyMappingType(0);
				if(mappingType == MappingType.RELATION) {
					Some(pm);
				} else {
				  None
				}
			});
		} else {
		  Nil
		}
		result;
	}
	
	override def isPossibleInstance(uri:String ) : Boolean = {
		var result = false;
		
		val subjectMapTermMapType = this.subjectMap.termMapType;
		if(subjectMapTermMapType == Constants.MorphTermMapType.TemplateTermMap) {
			val templateValues = this.subjectMap.getTemplateValues(uri);
			if(templateValues != null && templateValues.size() > 0) {
				result = true;
				for(value <- templateValues.values()) {
					if(value.contains("/")) {
						result = false;
					}
				}
			}
		} else {
			result = false;
			val errorMessage = "Can't determine whether " + uri + " is a possible instance of " + this.toString();
			logger.warn(errorMessage);
		}
		
		result;
	}
	
	override def getLogicalTableSize() : java.lang.Long = {
		this.logicalTable.getLogicalTableSize();
	}

	override def getMappedClassURIs() : java.util.Collection[String] = {
		this.subjectMap.getClassURIs();
	}

	override def getTableMetaData() : MorphTableMetaData = {
		this.logicalTable.getTableMetaData();
	}
	
	def getLogicalTable(): AbstractLogicalTable = {
	  this.logicalTable;      
	}

	override def getSubjectReferencedColumns() : java.util.List[String] = {
	  this.subjectMap.getReferencedColumns();
	}

}

object R2RMLTriplesMap {
	val logger = Logger.getLogger(this.getClass().getName());
	
	def apply(tmResource:Resource) : R2RMLTriplesMap = {
		val triplesMapName = tmResource.getLocalName();
		
		//LOGICAL TABLE
		val logicalTableStatement = tmResource.getProperty(Constants.R2RML_LOGICALTABLE_PROPERTY);
		if(logicalTableStatement == null) {
			val  errorMessage = "Missing rr:logicalTable";
			logger.error(errorMessage);
			throw new Exception(errorMessage);			  
		}
			
		val logicalTableStatementObject = logicalTableStatement.getObject();
		val logicalTableStatementObjectResource = logicalTableStatementObject.asInstanceOf[Resource];
		val logicalTable = R2RMLLogicalTable.parse(logicalTableStatementObjectResource);
//				try {
//					val conn = pOwner.getConn();
//					if(conn != null) {
//						logger.info("Building metadata for triples map: " + triplesMapName);
//						logicalTableAux.buildMetaData(conn);
//						logger.info("metadata built.");						
//					}
//					logicalTableAux
//				} catch{
//				  case e:Exception => {
//				    logger.error(e.getMessage());
//				    logicalTableAux
//				  }
//				}

		//rr:subjectMap
		val subjectMaps = R2RMLSubjectMap.extractSubjectMaps(tmResource);
		if(subjectMaps == null) {
			val errorMessage = "Missing rr:subjectMap";
			logger.error(errorMessage);
			throw new Exception(errorMessage);
		}
		if(subjectMaps.size > 1) {
			val errorMessage = "Multiple rr:subjectMap predicates are not allowed";
			logger.error(errorMessage);
			throw new Exception(errorMessage);
		}
		val subjectMap = subjectMaps.iterator.next;
		
		//rr:predicateObjectMap SET
		val predicateObjectMapStatements = tmResource.listProperties(
		    Constants.R2RML_PREDICATEOBJECTMAP_PROPERTY);
		val predicateObjectMaps = if(predicateObjectMapStatements != null) {
			predicateObjectMapStatements.toList().map(predicateObjectMapStatement => {
				val predicateObjectMapStatementObjectResource =  
				  predicateObjectMapStatement.getObject().asInstanceOf[Resource];
				val predicateObjectMap = R2RMLPredicateObjectMap(
						predicateObjectMapStatementObjectResource); 
				predicateObjectMap;			  
			});
		} else {
		  Set.empty;
		};
		
		val tm = new R2RMLTriplesMap(logicalTable, subjectMap, predicateObjectMaps.toSet);
		tm.setResource(tmResource);
		tm;
	}	
}

	