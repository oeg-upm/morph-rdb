package es.upm.fi.dia.oeg.morph.base

import scala.collection.JavaConversions._
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.util.FileManager
import com.hp.hpl.jena.rdf.model.ResourceFactory
import com.hp.hpl.jena.rdf.model.Resource
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype
import java.text.SimpleDateFormat
import org.apache.log4j.Logger

class TriplePatternPredicateBounder(mappingFile : String
    , mapColumnsMetaData : java.util.Map[String, ColumnMetaData]) {
  
	val logger = Logger.getLogger("TriplePatternPredicateBounder");
	val mappingDocument = new R2RMLMappingDocument(mappingFile);
	val constants = new Constants();
	
	def this(mappingFile : String) {
		this(mappingFile, new java.util.HashMap[String, ColumnMetaData]());
	}
	
	def expandUnboundedPredicateTriplePattern(tp : Triple, triplesMapResource : Resource) 
		: java.util.Map[Resource, java.util.List[String]] = {
		val pms = mappingDocument.getPredicateObjectMapResources(triplesMapResource);
		val result = this.checkExpandedTriplePatternList(tp, triplesMapResource, pms);
		result;
	}

	def checkExpandedTriplePatternList(tp : Triple, triplesMapResource : Resource, 
	    predicateObjectMapResources : List[Resource]) : java.util.Map[Resource, java.util.List[String]] = {
		val result : Map[Resource, java.util.List[String]] = {
			if(predicateObjectMapResources.isEmpty) {
				Map();
			} else {
				val pmsHead = predicateObjectMapResources.head;
				val pmsTail = predicateObjectMapResources.tail;
				val resultHead = this.checkExpandedTriplePattern(tp, triplesMapResource, pmsHead);
				val mapResultHead = Map(pmsHead -> resultHead);
				val mapResultTail = this.checkExpandedTriplePatternList(tp, triplesMapResource, pmsTail);
				mapResultHead ++ mapResultTail; 
			}	    
		}
		result
	}

	def checkExpandedTriplePattern(tp : Triple, triplesMapResource : Resource, 
	    predicateObjectMapResource : Resource) : java.util.List[String] = {
		val tpObject = tp.getObject();
		var result : List[String] = Nil;
		val objectMapResource = mappingDocument.getObjectMapResource(predicateObjectMapResource);
		val refObjectMapResource = mappingDocument.getRefObjectMapResource(predicateObjectMapResource);

		if(tpObject.isLiteral()) {
			val objectLiteralValue = tpObject.getLiteral().getValue();

			if(refObjectMapResource != null) {
				val errorMessage = "triple.object is a literal, but RefObjectMap is specified instead of ObjectMap";
				result = result ::: List(errorMessage);
			}
			
			if(objectMapResource != null) {
				val objectMapTermMapType = mappingDocument.getTermMapType(objectMapResource);
				val objectMapTermType = mappingDocument.getTermTypeResource(objectMapResource).getURI(); 
				if(objectMapTermType.equals(Constants.R2RML_TERMTYPE_IRI_URI)) {
					val errorMessage = "triple.object " + tp + " is a literal, but the mapping " + predicateObjectMapResource + " specifies URI.";
					result = result ::: List(errorMessage);
				}
				
				objectMapTermMapType match {
					case mappingDocument.TermMapType.ColumnTermMap => {
						val rrDatatypeResource = mappingDocument.getDatatypeResource(objectMapResource);
						val objectMapDatatype = {
							if(rrDatatypeResource == null) {
								if(this.mapColumnsMetaData == null) {
									null
								} else {
									val tableName = mappingDocument.getRRLogicalTableTableName(
									    triplesMapResource);
									val rrColumnResource = 
										mappingDocument.getRRColumnResource(objectMapResource);
									val columnName = rrColumnResource.getLiteral().getValue().toString();
									val columnMetaData = this.mapColumnsMetaData(columnName);
									if(columnMetaData == null) {
										null
									} else {
										val objectDatatypeFromFromMetaData = 
										  columnMetaData.dataType;
										objectDatatypeFromFromMetaData									  
									}
								}
							} else {
								rrDatatypeResource.asLiteral().getValue().toString();
							} 
						}
						
						if(objectMapDatatype != null) {
							
							objectMapDatatype match {
								case mappingDocument.XSDIntegerURI => {
									try {
										objectLiteralValue.asInstanceOf[Integer];
									} catch {
										case e:Exception => {
											val errorMessage = "triple.object " + tp + " not an integer, but the mapping " + predicateObjectMapResource + " specified mapped column is integer";
											result = result ::: List(errorMessage);
										}
									}
								}
								case mappingDocument.XSDDoubleURI => {
									try {
										objectLiteralValue.asInstanceOf[Double];
									} catch {
										case e:Exception => {
											val errorMessage = "triple.object " + tp + " not a double, but the mapping " + predicateObjectMapResource + " specified mapped column is double";
											result = result ::: List(errorMessage);
										}
									}
								}
								case mappingDocument.XSDDateTimeURI => {
									val dateFormat = new SimpleDateFormat("yyyy/MM/dd");
									try {
										dateFormat.parse(objectLiteralValue.toString());
									} catch {
										case e:Exception => {
											val errorMessage = "triple.object " + tp + " not a datetime, but the mapping " + predicateObjectMapResource + " specified mapped column is datetime";
											result = result ::: List(errorMessage);
										}
									}							
								}
								case _ => {}
							}					  
						}
					}
					case _ => {}
				}
			}		  
		} else if(tpObject.isURI()) {
			val tpObjectURI = tpObject.getURI();
			

			if(objectMapResource != null && refObjectMapResource == null) {
				val objectMapTermType = mappingDocument.getTermTypeResource(objectMapResource).getURI();
				
				objectMapTermType match {
					case Constants.R2RML_LITERAL_URI => {
						val errorMessage = "triple.object " + tp + " is an URI, but the mapping " + objectMapResource + " specifies literal";
						result = result ::: List(errorMessage);
					}
					case _ => {}
				}

				val objectMapTermMapType = mappingDocument.getTermMapType(objectMapResource); 
				objectMapTermMapType match {
					case mappingDocument.TermMapType.ColumnTermMap => {
						val rrDatatypeResource = mappingDocument.getDatatypeResource(objectMapResource);
						val objectMapDatatype = {
							if(rrDatatypeResource == null) {
									val logicalTableName = 
									  mappingDocument.getRRLogicalTableTableName(triplesMapResource);
									val rrColumnResource = 
										mappingDocument.getRRColumnResource(objectMapResource);
									val columnName = 
										rrColumnResource.getLiteral().getValue().toString();
									
									if(this.mapColumnsMetaData != null 
									    && this.mapColumnsMetaData.contains(columnName)) {
										val columnMetaData = this.mapColumnsMetaData(columnName);
										val objectDatatypeFromFromMetaData = 
											columnMetaData.dataType;
										objectDatatypeFromFromMetaData										
									}
								} else {
								rrDatatypeResource.asLiteral().getValue().toString();
							}
						}
					  
						if(objectMapDatatype != null) {
							objectMapDatatype match {
								case mappingDocument.XSDIntegerURI 
								| mappingDocument.XSDDoubleURI 
								| mappingDocument.XSDDateTimeURI => {
									val tpObjectURI = tpObject.getURI();
									val rrColumnResource = mappingDocument.getRRColumnResource(
									    objectMapResource);
									val columnName = 
									  rrColumnResource.getLiteral().getValue().toString(); 
									val errorMessage = "Numeric/Datetime column : " + columnName + " can't be used for URI : " + tpObjectURI;
									result = result ::: List(errorMessage);
								}
								case _ => {}
							}						  
						}
					}
					case mappingDocument.TermMapType.TemplateTermMap => {
						val templateValues = mappingDocument.getTemplateValues(objectMapResource, tpObjectURI);
						if(templateValues.isEmpty) {
							val errorMessage = "tp object " + tpObjectURI + " doesn't match the template : " + objectMapResource;
							result = result ::: List(errorMessage);
						}
					}
					case _ => {}
				}
			} else if(objectMapResource == null && refObjectMapResource != null) {
				val parentTriplesMapResource = mappingDocument.getParentTriplesMapResource(refObjectMapResource);
				val parentTriplesMapSubjectMapResource = mappingDocument.getRRSubjectMapResource(parentTriplesMapResource);
				val templateValues = mappingDocument.getTemplateValues(parentTriplesMapSubjectMapResource, tpObjectURI);
				if(templateValues.isEmpty) {
					val errorMessage = "tp object " + tpObjectURI + " doesn't match the template : " + parentTriplesMapSubjectMapResource;
					result = result ::: List(errorMessage);
				}
			}
		}

		result
	}	

//	def setMapColumnsMetaData(pMapColumnsMetaData : java.util.Map[String, ColumnMetaData]) = {
//		this.mapColumnsMetaData = pMapColumnsMetaData;
//	}

}

