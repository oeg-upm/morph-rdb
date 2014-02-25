package es.upm.dia.fi.oeg.morph.r2rml.model

import scala.collection.JavaConversions._
import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument
import com.hp.hpl.jena.rdf.model.Resource
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.obdi.core.model.IRelationMapping
import es.upm.fi.dia.oeg.obdi.core.model.AbstractPropertyMapping
import es.upm.fi.dia.oeg.obdi.core.model.IAttributeMapping
import es.upm.fi.dia.oeg.obdi.core.model.AbstractRDB2RDFMapping.MappingType
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping
import es.upm.fi.dia.oeg.morph.base.sql.MorphDatabaseMetaData

class R2RMLPredicateObjectMap(val predicateMaps:List[R2RMLPredicateMap]
		, val objectMaps:List[R2RMLObjectMap], val refObjectMaps:List[R2RMLRefObjectMap]
		, objectMapTypes:List[R2RMLPredicateObjectMap.ObjectMapType.Value]
		, val graphMaps:Set[R2RMLGraphMap] ) extends AbstractPropertyMapping 
with IRelationMapping with  IAttributeMapping {
	val logger = Logger.getLogger(this.getClass().getName());
	var alias:String = null;

	def buildMetadata(dbMetadata:MorphDatabaseMetaData) = {
	  try {
		  if(this.predicateMaps != null) {
			  this.predicateMaps.foreach(pm => { 
			    if(pm != null) {pm.buildMetadata(dbMetadata)}});
		  }
		  
		  if(this.objectMaps != null) {
		    this.objectMaps.foreach(om => { 
		      if(om != null) {om.buildMetadata(dbMetadata)} })
		  }
		  
		  if(this.refObjectMaps != null) {
		    
		  }
	  } catch {
	    case e:Exception => {
	      e.printStackTrace()
	      logger.warn("Error building metadata for predicateobjectmap:" + this);
	    }
	  }
	}
	
	def getMappedPredicateName(index:Int ) : String = {
		val result = if(this.predicateMaps != null && !this.predicateMaps.isEmpty()) {
			this.predicateMaps.get(index).getOriginalValue();
		} else {
			 null;
		}
		result;
	}

	def getObjectMap(index:Int) : R2RMLObjectMap = {
		val result = if(this.objectMaps != null && !this.objectMaps.isEmpty()) {
			this.objectMaps.get(index);
		} else {
			null;
		}
		result;
	}

	def getObjectMapType(index:Int) : R2RMLPredicateObjectMap.ObjectMapType.Value  = {
		val result = if(this.objectMapTypes != null && !this.objectMapTypes.isEmpty()) {
			this.objectMapTypes.get(index);
		} else {
			null;
		}
		result;
	}

	def getPredicateMap(index:Int ) : R2RMLPredicateMap  = {
		val result = if(this.predicateMaps != null && !this.predicateMaps.isEmpty()) {
			predicateMaps.get(index);
		} else {
			null;
		}
		result;
	}

	def getPropertyMappingID() : String  = {
		// TODO Auto-generated method stub
		null;
	}

	override def getPropertyMappingType(index:Int ) : MappingType = {
		val result = if(this.objectMaps != null && !this.objectMaps.isEmpty() 
				&& this.objectMaps.get(index) != null) {
			val objectMapTermType = this.objectMaps(index).termType;
			if(objectMapTermType.equals(Constants.R2RML_LITERAL_URI)) {
				MappingType.ATTRIBUTE;
			} else {
				MappingType.RELATION;
			}
		} else if(this.refObjectMaps != null && !this.refObjectMaps.isEmpty() 
				&& this.refObjectMaps.get(index) != null) {
			MappingType.RELATION;
		} else {
			null;
		}
		result;
	}

	def getRangeClassMapping(index:Int ) : String = {
		val result = if(this.refObjectMaps != null && !this.refObjectMaps.isEmpty() 
				&& this.refObjectMaps.get(index) != null) {
			this.refObjectMaps.get(index).getParentTripleMapName();
		} else {
			null;
		}
		result;
	}

	def getRefObjectMap(index:Int ) : R2RMLRefObjectMap = {
		val result=	if(this.refObjectMaps != null && !this.refObjectMaps.isEmpty()) {
			this.refObjectMaps.get(index);
		} else {
			null;
		}
		result;
	}

	def getRelationName() : String = {
		// TODO Auto-generated method stub
		logger.warn("TODO: Implement getRelationName");
		null;
	}

	override def toString() : String = {
		val result = "R2RMLPredicateObjectMap [predicateMaps=" + predicateMaps + ", objectMaps=" + objectMaps + ", refObjectMaps=" + refObjectMaps + ", objectMapTypes=" + objectMapTypes + "]";
		result;
	}

	def getAlias() : String = {
		alias;
	}

	def setAlias(alias:String ) = {
		this.alias = alias;
	}

	override def getRangeClassMapping() : String = {
		// TODO Auto-generated method stub
		null;
	}

	override def getMappedPredicateNames() : java.util.Collection[String] = {
		val result = this.predicateMaps.map(pm => {
			pm.getOriginalValue();
		});
		
		result;
	}
  
	def getAttributeName() : String = {
		// TODO Auto-generated method stub
		logger.warn("TODO: Implement getAttributeName");
		null;
	}	
}

object R2RMLPredicateObjectMap {
  	object ObjectMapType extends Enumeration {
		type ObjectMapType = Value
		val ObjectMap, RefObjectMap = Value
	}
  	
  	

	
	def extractObjectMaps(resource:Resource) = {
		val omStatements = resource.listProperties(Constants.R2RML_OBJECTMAP_PROPERTY);
		val objectMaps1 = if(omStatements != null) {
			omStatements.toList().flatMap(omStatement => {
				if(omStatement != null) {
					val omStatementObject = omStatement.getObject().asInstanceOf[Resource];
					val objectMapTuple = if(R2RMLRefObjectMap.isRefObjectMap(omStatementObject)) {
						val rom = R2RMLRefObjectMap(omStatementObject);
						(R2RMLPredicateObjectMap.ObjectMapType.RefObjectMap, null, rom);
					} else {
						val objectMap = new R2RMLObjectMap(omStatementObject);
						(R2RMLPredicateObjectMap.ObjectMapType.ObjectMap, objectMap, null);
					}
					Some(objectMapTuple)
				} else {
				  None
				}			  
			})
		} else {
		  Nil;
		}

		val objectStatements = resource.listProperties(Constants.R2RML_OBJECT_PROPERTY);
		val objectMaps2 = if(objectStatements != null) {
		  objectStatements.toList().flatMap(objectStatement => {
				if(objectStatement != null) {
					val constantValueObject = objectStatement.getObject().toString();
					val objectMap = new R2RMLObjectMap(constantValueObject);
					Some(ObjectMapType.ObjectMap, objectMap, null);
				} else {
				  None
				}		    
		  })
		} else {
		  Nil
		}
		
		val objectMaps = objectMaps1.toList ::: objectMaps2.toList;
		objectMaps
	}


	
	def apply(resource:Resource) : R2RMLPredicateObjectMap = {
		
		val predicateMaps = R2RMLPredicateMap.extractPredicateMaps(resource);
		val tupleObjectMaps = this.extractObjectMaps(resource);
		val refObjectMaps = tupleObjectMaps.map(x => x._3);
		val objectMaps = tupleObjectMaps.map(x => x._2);
		val objectMapTypes = tupleObjectMaps.map(x => x._1);
		val graphMaps = R2RMLGraphMap.extractGraphMaps(resource);
//		val graphMap = if(graphMaps != null && !graphMaps.isEmpty) {
//			this.extractGraphMaps(resource, parent)(0);  
//		} else {
//			null;
//		}
		
		
		val pom = new R2RMLPredicateObjectMap(predicateMaps, objectMaps, refObjectMaps
		    , objectMapTypes, graphMaps);
		pom;
	}
	
	def extractPredicateObjectMaps(resource:Resource) : Set[R2RMLPredicateObjectMap] = {
		val predicateObjectMapStatements = resource.listProperties(
		    Constants.R2RML_PREDICATEOBJECTMAP_PROPERTY);	  
		val predicateObjectMaps = if(predicateObjectMapStatements != null) {
			predicateObjectMapStatements.toList().map(predicateObjectMapStatement => {
				val predicateObjectMapStatementObjectResource =  
				  predicateObjectMapStatement.getObject().asInstanceOf[Resource];
				val predicateObjectMap = R2RMLPredicateObjectMap(predicateObjectMapStatementObjectResource); 
				predicateObjectMap;			  
			});
		} else {
		  Set.empty;
		}
		predicateObjectMaps.toSet;
	}
}