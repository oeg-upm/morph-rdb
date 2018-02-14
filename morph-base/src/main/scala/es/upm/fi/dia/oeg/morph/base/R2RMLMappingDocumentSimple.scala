package es.upm.fi.dia.oeg.morph.base

import scala.collection.JavaConversions._
//import com.hp.hpl.jena.rdf.model.ResourceFactory
import org.apache.jena.rdf.model.ResourceFactory;
//import com.hp.hpl.jena.datatypes.xsd.XSDDatatype
import org.apache.jena.datatypes.xsd.XSDDatatype;
//import com.hp.hpl.jena.rdf.model.ModelFactory
import org.apache.jena.rdf.model.ModelFactory;
//import com.hp.hpl.jena.util.FileManager
import org.apache.jena.util.FileManager;
//import com.hp.hpl.jena.rdf.model.Resource
import org.apache.jena.rdf.model.Resource;
import org.slf4j.LoggerFactory

class R2RMLMappingDocumentSimple(mappingFile : String) {
val logger = LoggerFactory.getLogger(this.getClass());
		
//	object MorphTermMapType extends Enumeration {
//		type MorphTermMapType = Value
//				val ConstantTermMap, ColumnTermMap, TemplateTermMap, InvalidTermMapType = Value
//	}

	


	
	val XSDIntegerURI = XSDDatatype.XSDinteger.getURI();
	val XSDDoubleURI = XSDDatatype.XSDdouble.getURI();
	val XSDDateTimeURI = XSDDatatype.XSDdateTime.getURI();
	var datatypesMap:Map[String, String] = Map();
	datatypesMap += ("INT" -> XSDIntegerURI);
	datatypesMap += ("DOUBLE" -> XSDDoubleURI);
	datatypesMap += ("DATETIME" -> XSDDateTimeURI);
	
	val model = ModelFactory.createDefaultModel();
	val in = FileManager.get().open( mappingFile );
	model.read(in, null, "TURTLE");

	def getTriplesMapResourcesByRRClass(classURI : String) = {
		var result : List[Resource] = Nil;
	  
		val triplesMapResources = model.listSubjectsWithProperty(Constants.R2RML_SUBJECTMAP_PROPERTY);
		if(triplesMapResources != null) {
			val triplesMapsList = triplesMapResources.toList();
			for(triplesMapResource <- triplesMapsList) yield {
			  val subjectMapResource = triplesMapResource.getPropertyResourceValue(Constants.R2RML_SUBJECTMAP_PROPERTY);
			  if(subjectMapResource != null) {
				  val rrClassResource = subjectMapResource.getPropertyResourceValue(Constants.R2RML_CLASS_PROPERTY);
				  if(rrClassResource != null) {
					  val subjectMapClassURI = rrClassResource.getURI(); 
					  if(classURI.equals(subjectMapClassURI)) {
						  result = result :::result ::: List(triplesMapResource);
					  }				    
				  }
			  }
			}		  
		}

		result;
	}

	def getPredicateObjectMapResources(triplesMapResources : List[Resource]) : List[Resource] =
		triplesMapResources.flatMap(this.getPredicateObjectMapResources(_))

		
	def getPredicateObjectMapResources(triplesMapResource : Resource) : List[Resource]= {
		val predicateObjectMapStatementsIterator = triplesMapResource.listProperties(Constants.R2RML_PREDICATEOBJECTMAP_PROPERTY);
		val predicateObjectMapStatements = predicateObjectMapStatementsIterator.toList().toList;
		for(predicateObjectMapResource <- predicateObjectMapStatements) {
		  val predicateObjectMapResourceObject = predicateObjectMapResource.getObject();
		  val predicateObjectMapResourceObjectResource = predicateObjectMapResourceObject.asResource();
		}
		
		val result = for {predicateObjectMapStatement <- predicateObjectMapStatements} 
			yield predicateObjectMapStatement.getObject().asResource();
		result;
	}
	
	def getRRLogicalTable(triplesMapResource : Resource) = {
	  val rrLogicalTableResource = triplesMapResource.getPropertyResourceValue(Constants.R2RML_LOGICALTABLE_PROPERTY);
	  rrLogicalTableResource;
	}

	def getRRSubjectMapResource(triplesMapResource : Resource) = {
		val rrSubjectMapResource = triplesMapResource.getPropertyResourceValue(Constants.R2RML_SUBJECTMAP_PROPERTY);
		rrSubjectMapResource;
	}

	def getRRLogicalTableTableName(triplesMapResource : Resource) = {
		val rrLogicalTableResource = this.getRRLogicalTable(triplesMapResource);
		val rrTableNameResource = rrLogicalTableResource.getPropertyResourceValue(Constants.R2RML_TABLENAME_PROPERTY);
		val result = {
			if( rrTableNameResource != null) {
				val tableName = rrTableNameResource.asLiteral().getValue().toString();
				tableName;
			} else {
				null;
			}		  
		}
		result;
	}
	
	def getObjectMapResource(predicateObjectMapResource : Resource) = {
	  val objectMapResource = predicateObjectMapResource.getPropertyResourceValue(Constants.R2RML_OBJECTMAP_PROPERTY);
	  val parentTriplesMap = objectMapResource.getPropertyResourceValue(Constants.R2RML_PARENTTRIPLESMAP_PROPERTY);
	  val result : Resource = {
		  if(parentTriplesMap == null) {
			  objectMapResource
		  } else {
			  null;
		  }
	  }
	  
	  result;
	}

	def getRefObjectMapResource(predicateObjectMapResource : Resource) = {
	  val objectMapResource = predicateObjectMapResource.getPropertyResourceValue(Constants.R2RML_OBJECTMAP_PROPERTY);
	  val parentTriplesMap = objectMapResource.getPropertyResourceValue(Constants.R2RML_PARENTTRIPLESMAP_PROPERTY);
	  val result : Resource = {
		  if(parentTriplesMap != null) {
			  objectMapResource
		  } else {
			  null;
		  }
	  }
	  
	  result;
	}
		
	def getParentTriplesMapResource(objectMapResource : Resource) = {
	  val parentTriplesMapResource = objectMapResource.getPropertyResourceValue(Constants.R2RML_PARENTTRIPLESMAP_PROPERTY);
	  parentTriplesMapResource;
	}
	
	def getParentTriplesMapLogicalTableResource(objectMapResource : Resource) = {
	  val parentTriplesMapResource = this.getParentTriplesMapResource(objectMapResource);
	  val parentTriplesMapLogicalTableResource = this.getRRLogicalTable(Constants.R2RML_LOGICALTABLE_PROPERTY); 
	  parentTriplesMapLogicalTableResource
	}

	def getTermTypeResource(termMapResource : Resource) = {
	  val termTypeResource = termMapResource.getPropertyResourceValue(Constants.R2RML_TERMTYPE_PROPERTY);
	  termTypeResource;
	}

	def getRRColumnResource(termMapResource : Resource) = {
	  val rrColumnResource = termMapResource.getProperty(Constants.R2RML_COLUMN_PROPERTY);
	  rrColumnResource;
	}

	def getRRTemplateResource(termMapResource : Resource) = {
	  val rrTemplateResource = termMapResource.getProperty(Constants.R2RML_TEMPLATE_PROPERTY);
	  rrTemplateResource.getObject();
	}

	
	def getDatatypeResource(termMapResource : Resource) = {
	  val datatypeResource = termMapResource.getPropertyResourceValue(Constants.R2RML_DATATYPE_PROPERTY);
	  datatypeResource;
	}

	
	
	def getTermMapType(termMapResource : Resource) = {
		if(termMapResource == null) {
			logger.debug("termMapResource is null");
		}
		
		val props = termMapResource.listProperties().toList();

		val termMapType = {
			val constantResource = termMapResource.getProperty(Constants.R2RML_CONSTANT_PROPERTY);
			if(constantResource != null) {
				Constants.MorphTermMapType.ConstantTermMap;
			} else {
				val columnResource = termMapResource.getProperty(Constants.R2RML_COLUMN_PROPERTY);
				if(columnResource != null) {
					Constants.MorphTermMapType.ColumnTermMap;
				} else {
					val templateResource = termMapResource.getProperty(Constants.R2RML_TEMPLATE_PROPERTY);
					if(templateResource != null) {
						Constants.MorphTermMapType.TemplateTermMap;
					} else {
						Constants.MorphTermMapType.InvalidTermMapType;
					}
				}
			}
		}
		termMapType;
	}
	
	def getTemplateValues(termMapResource : Resource, uri : String ) : Map[String, String] = {
		val termMapValueType = this.getTermMapType(termMapResource);

		val result : Map[String, String] = {
			if(termMapValueType == Constants.MorphTermMapType.TemplateTermMap) {
				val templateString = this.getRRTemplateResource(termMapResource).asLiteral().getValue().toString();
				val matchedTemplate = RegexUtility.getTemplateMatching(templateString, uri);
				matchedTemplate.toMap;
			} else {
			  Map();
			}
		}

		result;
	}	
	
}