package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import scala.collection.JavaConversions._
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import org.apache.log4j.Logger
import java.util.Collection
import es.upm.fi.dia.oeg.morph.base.DBUtility
import java.sql.ResultSet
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.GeneralUtility
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype
import java.sql.ResultSetMetaData
import java.sql.Connection
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLPredicateObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLRefObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.base.RegexUtility
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLConstant
import Zql.ZConstant
import es.upm.fi.dia.oeg.morph.base.sql.DatatypeMapper
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLUtility
import es.upm.fi.dia.oeg.morph.base.sql.IQuery
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLLogicalTable
import com.hp.hpl.jena.rdf.model.RDFNode
import com.hp.hpl.jena.rdf.model.AnonId
import com.hp.hpl.jena.vocabulary.RDF
import com.hp.hpl.jena.rdf.model.Literal
import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.MorphR2RMLElementVisitor
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLSubjectMap

class MorphRDBDataTranslator(md:R2RMLMappingDocument, materializer:MorphBaseMaterializer
    , unfolder:MorphRDBUnfolder, dataSourceReader:MorphRDBDataSourceReader
    , connection:Connection, properties:MorphProperties) 
extends MorphBaseDataTranslator(md, materializer , unfolder, dataSourceReader
    , connection, properties) 
with MorphR2RMLElementVisitor {
	override val logger = Logger.getLogger(this.getClass().getName());

	override def processCustomFunctionTransformationExpression(
			argument:Object ) : Object = {
		null;
	}

	override def translateData(triplesMap:MorphBaseClassMapping) : Unit = {
		val query = this.unfolder.unfoldConceptMapping(triplesMap);
		this.generateRDFTriples(triplesMap, query);
		null;	  
	}
	
	override def translateData(triplesMaps:Iterable[MorphBaseClassMapping]) : Unit = {
		for(triplesMap <- triplesMaps) {
			try {
			  this.visit(triplesMap.asInstanceOf[R2RMLTriplesMap]);
				//triplesMap.asInstanceOf[R2RMLTriplesMap].accept(this);
			} catch {
			  case e:Exception => {
				logger.error("error while translating data of triplesMap : " + triplesMap);
				if(e.getMessage() != null) {
					logger.error("error message = " + e.getMessage());
				}
				
				//e.printStackTrace();
				throw new Exception(e.getMessage(), e);			    
			  }
			}
		}
	}
	
	override def translateData(mappingDocument:MorphBaseMappingDocument ) = {
		val conn = this.connection
		
		val triplesMaps = mappingDocument.classMappings
		if(triplesMaps != null) {
			this.translateData(triplesMaps);
			//DBUtility.closeConnection(conn, "R2RMLDataTranslator");
		}
	}

//	def translateObjectMap(objectMap:R2RMLTermMap, rs:ResultSet
//	    , mapColumnType:Map[String, String] , subjectGraphName:String
//	    , predicateobjectGraphName:String, predicateMapUnfoldedValue:String
//	    ,  pObjectMapUnfoldedValue:String) = {
//
//		var objectMapUnfoldedValue = pObjectMapUnfoldedValue;
//		
//		if(objectMap != null && pObjectMapUnfoldedValue != null) {
//			val objectMapTermType = objectMap.termType;
//
//			if(Constants.R2RML_IRI_URI.equalsIgnoreCase(objectMapTermType)) {
//				try {
//					objectMapUnfoldedValue = GeneralUtility.encodeURI(objectMapUnfoldedValue);
//				} catch {
//				  case e:Exception => {
//				    logger.warn("Error encoding object value : " + objectMapUnfoldedValue);
//				  }
//				}					
//			}
//
//			objectMapTermType match {
//			  case Constants.R2RML_LITERAL_URI => {
//					val datatypeFromMapping = objectMap.datatype;
//					val language = objectMap.languageTag;
//					
//					val datatype = if(objectMap.termMapType == Constants.MorphTermMapType.ColumnTermMap) {
//						if(datatypeFromMapping == null) {
//							val columnName = objectMap.getColumnName();
//							//datatype = mapColumnType.get(columnName);
//							val dbType = this.properties.databaseType;
//							MorphSQLUtility.getXMLDatatype(columnName, mapColumnType, dbType);
//						} else {
//						  datatypeFromMapping
//						}
//					} else {
//					  datatypeFromMapping
//					}
//					
//					if(datatype != null) {
//					  val xsdDataTimeURI = XSDDatatype.XSDdateTime.getURI().toString();
//					  val xsdBooleanURI = XSDDatatype.XSDboolean.getURI().toString();
//					  
//					  datatype match {
//					    case xsdDataTimeURI => {
//							objectMapUnfoldedValue = objectMapUnfoldedValue.trim().replaceAll(" ", "T");
//						} 
//					    case xsdBooleanURI => {
//							if(objectMapUnfoldedValue.equalsIgnoreCase("T") 
//									|| objectMapUnfoldedValue.equalsIgnoreCase("True")
//									|| objectMapUnfoldedValue.equalsIgnoreCase("1")) {
//								objectMapUnfoldedValue = "true";
//							} else if(objectMapUnfoldedValue.equalsIgnoreCase("F") 
//									|| objectMapUnfoldedValue.equalsIgnoreCase("False")
//									|| objectMapUnfoldedValue.equalsIgnoreCase("0")) {
//								objectMapUnfoldedValue = "false";
//							} else {
//								objectMapUnfoldedValue = "false";
//							}
//						}						    
//					  }
//					}
//					
//					objectMapUnfoldedValue = GeneralUtility.encodeLiteral(objectMapUnfoldedValue);
//					if(this.properties != null) {
//						if(this.properties.literalRemoveStrangeChars) {
//							objectMapUnfoldedValue = GeneralUtility.removeStrangeChars(objectMapUnfoldedValue);
//						}
//					}
//					
//					if(subjectGraphName == null && predicateobjectGraphName == null) {
//						this.materializer.materializeDataPropertyTriple(predicateMapUnfoldedValue
//								, objectMapUnfoldedValue, datatype, language, null );
//					} else {
//						if(subjectGraphName != null) {
//							this.materializer.materializeDataPropertyTriple(
//									predicateMapUnfoldedValue, objectMapUnfoldedValue
//									, datatype, language, subjectGraphName );
//						}
//						
//						if(predicateobjectGraphName != null) {
//							if(subjectGraphName == null || 
//									!predicateobjectGraphName.equals(subjectGraphName)) {
//								this.materializer.materializeDataPropertyTriple(
//										predicateMapUnfoldedValue, objectMapUnfoldedValue
//										, datatype, language, predicateobjectGraphName);							
//							}
//						}
//					}
//				} 
//			  case Constants.R2RML_IRI_URI => {
//					try {
//						objectMapUnfoldedValue = GeneralUtility.encodeURI(objectMapUnfoldedValue);
//						if(subjectGraphName == null && predicateobjectGraphName == null) {
//							this.materializer.materializeObjectPropertyTriple(predicateMapUnfoldedValue, objectMapUnfoldedValue, false, null );
//						} else {
//							if(subjectGraphName != null) {
//								this.materializer.materializeObjectPropertyTriple(predicateMapUnfoldedValue, objectMapUnfoldedValue, false, subjectGraphName );
//							}
//							if(predicateobjectGraphName != null) {
//								if(subjectGraphName == null || 
//										!predicateobjectGraphName.equals(subjectGraphName)) {
//									this.materializer.materializeObjectPropertyTriple(predicateMapUnfoldedValue, objectMapUnfoldedValue, false, predicateobjectGraphName );
//								}
//							}
//						}					
//					} catch {
//					  case e:Exception => {}
//						
//					}
//					
//	
//				} 
//			  case Constants.R2RML_BLANKNODE_URI => {
//					if(subjectGraphName == null && predicateobjectGraphName == null) {
//						this.materializer.materializeObjectPropertyTriple(predicateMapUnfoldedValue, objectMapUnfoldedValue, true, null );
//					} else {
//						if(subjectGraphName != null) {
//							this.materializer.materializeObjectPropertyTriple(predicateMapUnfoldedValue, objectMapUnfoldedValue, true, subjectGraphName );
//						}
//						if(predicateobjectGraphName != null) {
//							if(subjectGraphName == null || 
//									!predicateobjectGraphName.equals(subjectGraphName)) {
//								this.materializer.materializeObjectPropertyTriple(predicateMapUnfoldedValue, objectMapUnfoldedValue, true, predicateobjectGraphName );
//							}
//							
//						}
//					}					
//				} 
//			  case _ => {
//					logger.warn("Undefined term type for object map : " + objectMap);
//				}			  
//			}
//
//
//		}
//	}

	def visit( logicalTable:R2RMLLogicalTable) : Object = {
		// TODO Auto-generated method stub
		null;
	}

	override def visit(mappingDocument:R2RMLMappingDocument) : Object = {
		try {
			this.translateData(mappingDocument);
		} catch {
		  case e:Exception => {
			e.printStackTrace();
			logger.error("error during data translation process : " + e.getMessage());
			throw new Exception(e.getMessage());		    
		  }
		}

		null;
	}

	def visit(objectMap:R2RMLObjectMap ) : Object = {
		// TODO Auto-generated method stub
		null;
	}

	def visit(refObjectMap:R2RMLRefObjectMap ) : Object  = {
		// TODO Auto-generated method stub
		null;
	}

	def visit(r2rmlTermMap:R2RMLTermMap) : Object = {
		// TODO Auto-generated method stub
		null;
	}


	
	def generateRDFTriples(logicalTable:R2RMLLogicalTable ,  sm:R2RMLSubjectMap
			, poms:Iterable[R2RMLPredicateObjectMap] , iQuery:IQuery) = {
		logger.info("Translating RDB data into RDF instances...");
		
		if(sm == null) {
			val errorMessage = "No SubjectMap is defined";
			logger.error(errorMessage);
			throw new Exception(errorMessage);
		}
		
		val logicalTableAlias = logicalTable.alias;
		
		val conn = this.connection
		val timeout = this.properties.databaseTimeout;
		val sqlQuery = iQuery.toString();
		val rows = DBUtility.executeQuery(conn, sqlQuery, timeout);
		
		var mapXMLDatatype : Map[String, String] = Map.empty;
		var mapDBDatatype:Map[String, Integer]  = Map.empty;
		var rsmd : ResultSetMetaData = null;
		val datatypeMapper = new DatatypeMapper();
		
		try {
			rsmd = rows.getMetaData();
			val columnCount = rsmd.getColumnCount();
			for (i <- 0 until columnCount) {
				val columnName = rsmd.getColumnName(i+1);
				val columnType= rsmd.getColumnType(i+1);
				val mappedDatatype = datatypeMapper.getMappedType(columnType);
//				if(mappedDatatype == null) {
//					mappedDatatype = XSDDatatype.XSDstring.getURI();
//				}
				mapXMLDatatype += (columnName -> mappedDatatype);
				mapDBDatatype += (columnName -> new Integer(columnType));
			}
		} catch {
		  case e:Exception => {
			//e.printStackTrace();
			logger.warn("Unable to detect database columns!");		    
		  }
		}

		val classes = sm.classURIs;
		val sgm = sm.graphMaps;

		var i=0;
		while(rows.next()) {
			i = i+1;
			try {
				//translate subject map
				val subject = this.translateData(sm, rows, logicalTableAlias, mapXMLDatatype);
				if(subject == null) {
					val errorMessage = "null value in the subject triple!";
					logger.debug("null value in the subject triple!");
					throw new Exception(errorMessage);
				}
//				val subjectString = subject.toString();
//				this.materializer.createSubject(sm.isBlankNode(), subjectString);
				
				val subjectGraphs = sgm.map(sgmElement=> {
					val subjectGraphValue = this.translateData(sgmElement, rows, logicalTableAlias, mapXMLDatatype);
//					val subjectGraphValue = this.translateData(sgmElement, unfoldedSubjectGraph, mapXMLDatatype);
					val graphMapTermType = sgmElement.inferTermType;
					val subjectGraph = graphMapTermType match {
						case Constants.R2RML_IRI_URI => {
						  subjectGraphValue
						} 
						case _ => {
							val errorMessage = "GraphMap's TermType is not valid: " + graphMapTermType;
							logger.warn(errorMessage);
							throw new Exception(errorMessage);						  
						}
					}
					subjectGraph
				});
				

				//rdf:type
				classes.foreach(classURI => {
					val statementObject = this.materializer.model.createResource(classURI);
					if(subjectGraphs == null || subjectGraphs.isEmpty) {
//						this.materializer.materializeRDFTypeTriple(subjectString, classURI, sm.isBlankNode(), null);
					  this.materializer.materializeQuad(subject, RDF.`type`, statementObject, null);
					  this.materializer.outputStream.flush();
					  
					} else {
						subjectGraphs.foreach(subjectGraph => {
//							this.materializer.materializeRDFTypeTriple(subjectString, classURI, sm.isBlankNode(), subjectGraph);
							this.materializer.materializeQuad(subject, RDF.`type`, statementObject, subjectGraph);
						  });
					}
				});
				  
				poms.foreach(pom => {
					val alias = if(pom.getAlias() == null) { logicalTableAlias; } 
					else { pom.getAlias() }
				  
					val predicates = pom.predicateMaps.map(predicateMap => {
						val predicateValue = this.translateData(predicateMap, rows, null, mapXMLDatatype);
//						val predicateValue = this.translateData(predicateMap, unfoldedPredicateMap, mapXMLDatatype);
						predicateValue;
					});
					
					val objects = pom.objectMaps.map(objectMap => {
						val objectValue = this.translateData(objectMap, rows, alias, mapXMLDatatype);
//						val objectValue = this.translateData(objectMap, unfoldedObjectMap, mapXMLDatatype);
						objectValue;
					});
					
					val refObjects = pom.refObjectMaps.map(refObjectMap => {
					  val parentTripleMapName = refObjectMap.getParentTripleMapName;
					  val parentTriplesMap = this.md.getParentTriplesMap(refObjectMap)
					  val parentSubjectMap = parentTriplesMap.subjectMap; 
					  val parentTableAlias = this.unfolder.mapRefObjectMapAlias.getOrElse(refObjectMap, null);
					  val parentSubjects = this.translateData(parentSubjectMap, rows, parentTableAlias, mapXMLDatatype)
					  parentSubjects
					})
					
					val pogm = pom.graphMaps;
					val predicateObjectGraphs = pogm.map(pogmElement=> {
					  val poGraphValue = this.translateData(pogmElement, rows, null, mapXMLDatatype);
//					  val poGraphValue = this.translateData(pogmElement, unfoldedPOGraphMap, mapXMLDatatype);
					  poGraphValue
					});

										    
					if(sgm.isEmpty && pogm.isEmpty) {
						predicates.foreach(predicatesElement => {
						  objects.foreach(objectsElement => {
						    this.materializer.materializeQuad(subject, predicatesElement, objectsElement, null)
						  });

						  refObjects.foreach(refObjectsElement => {
						    this.materializer.materializeQuad(subject, predicatesElement, refObjectsElement, null)
						  });
						});					  
					} else {
					  val unionGraphs = subjectGraphs ++ predicateObjectGraphs
					  unionGraphs.foreach(unionGraph => {
						predicates.foreach(predicatesElement => {
						  objects.foreach(objectsElement => {
						    unionGraphs.foreach(unionGraph => {
						      this.materializer.materializeQuad(subject, predicatesElement, objectsElement, unionGraph)
						    })
						  });

						  refObjects.foreach(refObjectsElement => {
						    this.materializer.materializeQuad(subject, predicatesElement, refObjectsElement, unionGraph)
						  });
						  
						});					    
					  })
					}

				});
				
			} catch {
			  case e:Exception => {
			    e.printStackTrace();
			    logger.error("error while translating data: " + e.getMessage());
			  }
			}
		}
		
		
		logger.info(i + " instances retrieved.");
		rows.close();

	}

//	def translateObjectMaps(subjectGraphName:String, predicateobjectGraphName:String
//	    , predicateMapUnfoldedValue:String, objectMaps:List[R2RMLObjectMap]
//	, rs:ResultSet, mapXMLDatatype : Map[String, String]
//	, logicalTableAlias:String, predicateObjectMap:R2RMLPredicateObjectMap) = {
//		if(objectMaps != null && !objectMaps.isEmpty()) {
//			for(objectMap <- objectMaps) {
//				//R2RMLObjectMap objectMap = predicateObjectMap.getObjectMap(0);
//				if(objectMap != null) {
//					//retrieve the alias from predicateObjectMap, not triplesMap!
//					val alias = if(predicateObjectMap.getAlias() == null) {
//						logicalTableAlias;
//					} else {
//					  predicateObjectMap.getAlias()
//					}
//					//String alias = triplesMap.getLogicalTable().getAlias();
//					
//					val objectMapUnfoldedValue = 
//							objectMap.getUnfoldedValue(rs, alias);
//					this.translateObjectMap(objectMap, rs, mapXMLDatatype
//							, subjectGraphName, predicateobjectGraphName
//							, predicateMapUnfoldedValue, objectMapUnfoldedValue
//							);
//				}												
//			}
//		}	  
//	}
	
//	def translateRefObjectMaps(subjectGraphName:String, predicateobjectGraphName:String
//	    , refObjectMaps:List[R2RMLRefObjectMap], rs:ResultSet
//	    , mapXMLDatatype : Map[String, String]) = {
//		if(refObjectMaps != null && !refObjectMaps.isEmpty()) {
//			for(refObjectMap <- refObjectMaps) {
//				if(refObjectMap != null) {
//					val r2rmlUnfolder = this.unfolder.asInstanceOf[R2RMLUnfolder];
//					val joinQueryAlias2 = if(r2rmlUnfolder.getMapRefObjectMapAlias().get(refObjectMap).isDefined) {
//					  r2rmlUnfolder.getMapRefObjectMapAlias().get(refObjectMap).get;
//					} else {
//					  null
//					}
//					
//					val parentTriplesMap = refObjectMap.getParentTriplesMap().asInstanceOf[R2RMLTriplesMap];
//					val parentSubjectMap =parentTriplesMap.subjectMap;
//					val parentSubjectValue = parentSubjectMap.getUnfoldedValue(rs, joinQueryAlias2);
//					if(parentSubjectValue != null) {
//						this.translateObjectMap(parentSubjectMap, rs, mapXMLDatatype, subjectGraphName
//								, predicateobjectGraphName, predicateMapUnfoldedValue, parentSubjectValue
//								);
//					}
//				}												
//			}
//		}	  
//	}
	
	def visit(triplesMap:R2RMLTriplesMap) : Object = {
//		String sqlQuery = triplesMap.accept(
//				new R2RMLElementUnfoldVisitor()).toString();
		this.translateData(triplesMap);
		null;
	}

	override def generateRDFTriples(cm:MorphBaseClassMapping , iQuery:IQuery ) = {
		val triplesMap = cm.asInstanceOf[R2RMLTriplesMap];
		val logicalTable = triplesMap.getLogicalTable().asInstanceOf[R2RMLLogicalTable];
		val sm = triplesMap.subjectMap;
		val poms = triplesMap.predicateObjectMaps;
		this.generateRDFTriples(logicalTable, sm, poms, iQuery);		
	}

	override def generateSubjects(cm:MorphBaseClassMapping, iQuery:IQuery) = {
		val triplesMap = cm.asInstanceOf[R2RMLTriplesMap];
		val logicalTable = triplesMap.getLogicalTable().asInstanceOf[R2RMLLogicalTable];
		val sm = triplesMap.subjectMap;
		this.generateRDFTriples(logicalTable, sm, Nil, iQuery);
		//conn.close();		
	}
	
	def createIRI(originalIRI:String) = {
	    var resultIRI = originalIRI;
	    try {
			resultIRI = GeneralUtility.encodeURI(resultIRI
			    , properties.mapURIEncodingChars, properties.uriTransformationOperation);
			if(this.properties != null) {
				if(this.properties.encodeUnsafeChars) {
				  resultIRI = GeneralUtility.encodeUnsafeChars(resultIRI);
				}
				
				if(this.properties.encodeReservedChars) {
					resultIRI = GeneralUtility.encodeReservedChars(resultIRI);
				}
			}
			this.materializer.model.createResource(resultIRI);
		} catch {
			case e:Exception => {
				logger.warn("Error translating object uri value : " + resultIRI);
				throw e
			}
		}
	}

	def translateDateTime(value:String) = {
	  value.toString().trim().replaceAll(" ", "T");
	}
	
	def translateBoolean(value:String) = {
		if(value.equalsIgnoreCase("T")  || value.equalsIgnoreCase("True") || value.equalsIgnoreCase("1")) {
  			"true";
  		} else if(value.equalsIgnoreCase("F") || value.equalsIgnoreCase("False") || value.equalsIgnoreCase("0")) {
  			"false";
  		} else {
  			"false";
  		}	  
	}
	
	def createLiteral(value:Object, datatype:Option[String]
	, language:Option[String]) : Literal = {
	    try {
			val encodedValueAux = GeneralUtility.encodeLiteral(value.toString());
//			val encodedValue = if(this.properties != null) {
//				if(this.properties.literalRemoveStrangeChars) {
//				  GeneralUtility.removeStrangeChars(encodedValueAux);
//				} else { encodedValueAux }
//			} else { encodedValueAux }
			val encodedValue = encodedValueAux;
			
			val valueWithDataType = if(datatype.isDefined ) {
				val xsdDateTimeURI = XSDDatatype.XSDdateTime.getURI().toString();
				val xsdBooleanURI = XSDDatatype.XSDboolean.getURI().toString();
					  
				datatype.get match {
					case xsdDateTimeURI => {
					  this.translateDateTime(encodedValue);
					} 
					case xsdBooleanURI => {
					  this.translateBoolean(encodedValue);
				  	}
					case _ => {
					  encodedValue
					}
				  }
			  } else { encodedValue }

			val result:Literal = if(language.isDefined) {
			  this.materializer.model.createLiteral(valueWithDataType, language.get);
			} else {
			  if(datatype.isDefined) {
			    this.materializer.model.createTypedLiteral(valueWithDataType, datatype.get);
			  } else {
			    this.materializer.model.createLiteral(valueWithDataType);
			  }
			}
			
//			val result:Literal = if(datatype.isDefined) {
//			  this.materializer.model.createTypedLiteral(encodedValue, datatype.get);
//			} else {
//				if(language.isDefined) {
//				  this.materializer.model.createLiteral(encodedValue, language.get);
//				} else {
//				  this.materializer.model.createLiteral(encodedValue);
//				}			  
//			}
			result
		} catch {
			case e:Exception => {
				logger.warn("Error translating object uri value : " + value);
				throw e
			}
		}
	}
	
//	def translateData2(termMap:R2RMLTermMap, originalValue:Object
//	    , mapXMLDatatype : Map[String, String]) = {
//		val translatedValue:String = termMap.inferTermType match {
//		  case Constants.R2RML_IRI_URI => {
//			 this.translateIRI(originalValue.toString());
//		  }
//		  case Constants.R2RML_LITERAL_URI => {
//			  this.translateLiteral(termMap, originalValue, mapXMLDatatype);
//		  }
//		  case Constants.R2RML_BLANKNODE_URI => {
//		    val resultBlankNode = GeneralUtility.createBlankNode(originalValue.toString());
//		    resultBlankNode
//		  } 
//		  case _ => {
//			  originalValue.toString()
//		  }
//		}
//		translatedValue
//	}
	
	def translateBlankNode(value:Object) = {
	  
	}
	
	def translateData(termMap:R2RMLTermMap, dbValue:Object, datatype:Option[String]
	//    , mapXMLDatatype : Map[String, String]
	) = {
		termMap.inferTermType match {
			case Constants.R2RML_IRI_URI => {
			  if(dbValue != null) { this.createIRI(dbValue.toString()); }
			  else { null }
			}
			case Constants.R2RML_LITERAL_URI => {
				if(dbValue != null) {
					this.createLiteral(dbValue, datatype, termMap.languageTag);
				}
				else { null }
			}
			case Constants.R2RML_BLANKNODE_URI => {
				if(dbValue != null ) {
					val anonId = new AnonId(dbValue.toString());
					this.materializer.model.createResource(anonId)				  
				} else { null }
			} 
			case _ => {
				null
			}
		}
	}
	
	def translateData(termMap:R2RMLTermMap, rs:ResultSet , logicalTableAlias:String 
	    , mapXMLDatatype : Map[String, String]
	) : RDFNode = {
		val dbType = this.properties.databaseType;
		val dbEnclosedCharacter = Constants.getEnclosedCharacter(dbType);
		val inferedTermType = termMap.inferTermType;
				
		val result = termMap.termMapType match {
			case Constants.MorphTermMapType.ColumnTermMap => {
			  val columnTermMapValue =
			    if(logicalTableAlias != null && !logicalTableAlias.equals("")) {
					val termMapColumnValueSplit = termMap.columnName.split("\\.");
					val columnName = termMapColumnValueSplit(termMapColumnValueSplit.length - 1).replaceAll("\"", dbEnclosedCharacter);;
					logicalTableAlias + "_" + columnName;
			    } else {
			    	termMap.columnName
			    }

			  val dbValueAux = this.getResultSetValue(termMap, rs, columnTermMapValue);
//			  val dbValue = dbValueAux match {
//				  case dbValueAuxString:String => {
//					  if(this.properties.transformString.isDefined) {
//					    this.properties.transformString.get match {
//						    case Constants.TRANSFORMATION_STRING_TOLOWERCASE => {
//						      dbValueAuxString.toLowerCase();
//						    }
//						    case Constants.TRANSFORMATION_STRING_TOUPPERCASE => {
//						      dbValueAuxString.toUpperCase();
//						    }
//						    case _ => { dbValueAuxString }
//					    }
//
//					  } 
//					  else { dbValueAuxString }
//				  }
//				  case _ => { dbValueAux }
//			  }
			  val dbValue = dbValueAux;
			  

				val datatype = if(termMap.datatype.isDefined) { termMap.datatype } 
				else {
				  val columnNameAux = termMap.columnName.replaceAll("\"", "");
				  val datatypeAux = mapXMLDatatype.get(columnNameAux)
				  datatypeAux
				}
				
			  	
				this.translateData(termMap, dbValue, datatype);
			} 
		  case Constants.MorphTermMapType.ConstantTermMap => {
				val datatype = if(termMap.datatype.isDefined) { termMap.datatype } else { None }		    
			  this.translateData(termMap, termMap.constantValue, datatype);
			} 
		  case Constants.MorphTermMapType.TemplateTermMap => {
				val datatype = if(termMap.datatype.isDefined) { termMap.datatype } else { None }		    
		    
				val  attributes = RegexUtility.getTemplateColumns(termMap.templateString, true);
				val replacements = attributes.flatMap(attribute => {
					val databaseColumn = if(logicalTableAlias != null) {
						val attributeSplit = attribute.split("\\.");
						if(attributeSplit.length >= 1) {
							val columnName = attributeSplit(attributeSplit.length - 1).replaceAll("\"", dbEnclosedCharacter);
							logicalTableAlias + "_" + columnName;
						} else {
						  logicalTableAlias + "_" + attribute;
						}
					} else {
						attribute;
					}
					
					val dbValueAux = this.getResultSetValue(termMap, rs, databaseColumn);
					  val dbValue = dbValueAux match {
						  case dbValueAuxString:String => {
							  if(this.properties.transformString.isDefined) {
							    this.properties.transformString.get match {
								    case Constants.TRANSFORMATION_STRING_TOLOWERCASE => {
								      dbValueAuxString.toLowerCase();
								    }
								    case Constants.TRANSFORMATION_STRING_TOUPPERCASE => {
								      dbValueAuxString.toUpperCase();
								    }
								    case _ => { dbValueAuxString }
							    }
		
							  } 
							  else { dbValueAuxString }
						  }
						  case _ => { dbValueAux }
					  }					
					if(dbValue != null) {
						val databaseValueString = dbValue.toString();
						Some(attribute -> databaseValueString);
					} else {
					  None
					}
				}).toMap

				if(replacements.isEmpty) {
				  null
				} else {
					val templateWithDBValue = RegexUtility.replaceTokens(termMap.templateString, replacements);
					if(templateWithDBValue != null) {
						this.translateData(termMap, templateWithDBValue, datatype);  
					} else { null }				  
				}
			}	
		}
		result

	}
	
	def getResultSetValue(termMap:R2RMLTermMap, rs:ResultSet, pColumnName:String ) : Object = {
		try {
			val dbType = this.properties.databaseType;
			val dbEnclosedCharacter = Constants.getEnclosedCharacter(dbType);
		  
			//val logicalTableMetaData = ownerTriplesMap.getLogicalTable();
			
			//val dbType = this.configurationProperties.databaseType;
			//val dbType = logicalTableMetaData.getTableMetaData.dbType;
			//val zConstant = MorphSQLConstant(pColumnName, ZConstant.COLUMNNAME, dbType);
			val zConstant = MorphSQLConstant(pColumnName, ZConstant.COLUMNNAME);
			val tableName = zConstant.table;
			//val columnNameAux = zConstant.column.replaceAll("\"", "")
			//val columnNameAux = zConstant.column.replaceAll(dbEnclosedCharacter, ""); //doesn't work for 9a
			val columnNameAux = zConstant.column 
			
			
			val columnName = {
				if(tableName != null) {
					tableName + "." + columnNameAux
				} else {
				  columnNameAux
				}
			}

			  
			val result = if(termMap.datatype == null) {
				rs.getString(columnName);
			} else if(termMap.datatype.equals(XSDDatatype.XSDdateTime.getURI())) {
				rs.getDate(columnName).toString();
			} else {
				rs.getObject(columnName);
			}
			result
		} catch {
		  case e:Exception => {
		    e.printStackTrace();
		    logger.error("error occured when translating result: " + e.getMessage());
		    null
		  }
		}
	}	
}