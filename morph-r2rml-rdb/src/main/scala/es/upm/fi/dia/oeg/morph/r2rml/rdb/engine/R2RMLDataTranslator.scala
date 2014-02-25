package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import scala.collection.JavaConversions._
import es.upm.fi.dia.oeg.morph.base.ConfigurationProperties
import es.upm.fi.dia.oeg.obdi.core.engine.AbstractDataTranslator
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.obdi.core.materializer.AbstractMaterializer
import java.util.Collection
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping
import es.upm.fi.dia.oeg.obdi.core.exception.QueryTranslatorException
import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument
import es.upm.fi.dia.oeg.morph.base.DBUtility
import java.sql.ResultSet
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.GeneralUtility
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLLogicalTable
import es.upm.fi.dia.oeg.obdi.core.engine.RDBReader
import java.sql.ResultSetMetaData
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLPredicateObjectMap
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLTermMap
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLObjectMap
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLRefObjectMap
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLSubjectMap
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLTermMap
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLTermMap
import es.upm.fi.dia.oeg.morph.base.RegexUtility
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLConstant
import Zql.ZConstant
import es.upm.fi.dia.oeg.morph.base.sql.DatatypeMapper
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLUtility

class R2RMLDataTranslator(properties:ConfigurationProperties) 
extends AbstractDataTranslator(properties:ConfigurationProperties ){
	val logger = Logger.getLogger(this.getClass().getName());

	override def processCustomFunctionTransformationExpression(
			argument:Object ) : Object = {
		null;
	}

	override def setMaterializer(materializer:AbstractMaterializer ) = {
		this.materializer = materializer;
		
	}

	override def translateData(triplesMaps:Collection[AbstractConceptMapping]) = {
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
				throw new QueryTranslatorException(e.getMessage(), e);			    
			  }
			}
		}
	}
	
	override def translateData(mappingDocument:AbstractMappingDocument ) = {
		val conn = this.properties.conn;
		
		val triplesMaps = mappingDocument.getConceptMappings();
		if(triplesMaps != null) {
			this.translateData(triplesMaps);
			DBUtility.closeConnection(conn, "R2RMLDataTranslator");
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

	def visit(mappingDocument:AbstractMappingDocument) : Object = {
		try {
			this.translateData(mappingDocument);
		} catch {
		  case e:Exception => {
			e.printStackTrace();
			logger.error("error during data translation process : " + e.getMessage());
			throw new QueryTranslatorException(e.getMessage());		    
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
			, poms:Collection[R2RMLPredicateObjectMap] , sqlQuery:String ) = {
		logger.info("Translating RDB data into RDF instances...");
		
		if(sm == null) {
			val errorMessage = "No SubjectMap is defined";
			logger.error(errorMessage);
			throw new Exception(errorMessage);
		}
		
		val logicalTableAlias = logicalTable.getAlias();
		
		val conn = if(this.properties.conn == null) {
			DBUtility.getLocalConnection(this.properties.databaseUser
					, this.properties.databaseName, this.properties.databasePassword
					, this.properties.databaseDriver, this.properties.databaseURL, 
					"R2RMLDataTranslator");
		} else {
		  this.properties.conn
		}
		
		val timeout = this.properties.databaseTimeout;
		val rows = RDBReader.evaluateQuery(sqlQuery, conn, timeout);
		
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

		
		val classes = sm.getClassURIs();
		val sgm = sm.getGraphMaps();

		var i=0;
		while(rows.next()) {
			i = i+1;
			try {
				//translate subject map
				val subject = this.translateValue(sm, rows, logicalTableAlias);
				if(subject == null) {
					val errorMessage = "null value in the subject triple!";
					logger.debug("null value in the subject triple!");
					throw new Exception(errorMessage);
				}
				val subjectString = subject.toString();
				this.materializer.createSubject(sm.isBlankNode(), subjectString);
				
				val subjectGraphs = sgm.map(sgmElement=> {
					val unfoldedSubjectGraph = this.translateValue(sgmElement, rows, logicalTableAlias);
					val subjectGraphValue = this.translateData(sgmElement, unfoldedSubjectGraph, mapXMLDatatype);
					val graphMapTermType = sgmElement.termType;
					val subjectGraph :String = graphMapTermType match {
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
				  subjectGraphs.foreach(subjectGraph => {
				    this.materializer.materializeRDFTypeTriple(
				        subjectString, classURI, sm.isBlankNode(), subjectGraph);
				  });
				});
				
				poms.foreach(pom => {
					val alias = if(pom.getAlias() == null) { logicalTableAlias; } 
					else { pom.getAlias() }
				  
					val predicates = pom.predicateMaps.map(predicateMap => {
						val unfoldedPredicateMap = this.translateValue(predicateMap, rows, null);
						val predicateValue = this.translateData(predicateMap, unfoldedPredicateMap, mapXMLDatatype);
						predicateValue;
					});
					
					val objects = pom.objectMaps.map(objectMap => {
						val unfoldedObjectMap = this.translateValue(objectMap, rows, alias);
						val objectValue = this.translateData(objectMap, unfoldedObjectMap, mapXMLDatatype);
						objectValue;
					});
					
					val pogm = pom.graphMaps;
					val predicateObjectGraphs = pogm.map(pogmElement=> {
					  val unfoldedPOGraphMap = this.translateValue(pogmElement, rows, null);
					  val poGraphValue = this.translateData(pogmElement, unfoldedPOGraphMap, mapXMLDatatype);
					  poGraphValue
					});

					val unionGraphs = if(sgm.isEmpty && pogm.isEmpty) {
					    Set("");  
					} else {
						subjectGraphs ++ predicateObjectGraphs
					}
										    
					predicates.foreach(predicatesElement => {
					  objects.foreach(objectsElement => {
					    unionGraphs.foreach(unionGraph => {
					      this.materializer.materializeQuad(subjectString, predicatesElement, objectsElement, unionGraph)
					    })
					  });
					});
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
		val r2rmlUnfolder = this.unfolder.asInstanceOf[R2RMLUnfolder];
		val sqlQuery = triplesMap.accept(r2rmlUnfolder).toString();
		this.generateRDFTriples(triplesMap, sqlQuery);
		null;
	}

	override def generateRDFTriples(cm:AbstractConceptMapping , sqlQuery:String ) = {
		val triplesMap = cm.asInstanceOf[R2RMLTriplesMap];
		val logicalTable = triplesMap.getLogicalTable().asInstanceOf[R2RMLLogicalTable];
		val sm = triplesMap.subjectMap;
		val poms = triplesMap.predicateObjectMaps;
		this.generateRDFTriples(logicalTable, sm, poms, sqlQuery);
		//conn.close();		
	}

	override def generateSubjects(cm:AbstractConceptMapping, sqlQuery:String) = {
		val triplesMap = cm.asInstanceOf[R2RMLTriplesMap];
		val logicalTable = triplesMap.getLogicalTable().asInstanceOf[R2RMLLogicalTable];
		val sm = triplesMap.subjectMap;
		this.generateRDFTriples(logicalTable, sm, null, sqlQuery);
		//conn.close();		
	}
	
	def translateIRI(termMap:R2RMLTermMap, originalIRI:String) : String = {
	    var resultIRI = originalIRI;
		try {
			resultIRI = GeneralUtility.encodeURI(resultIRI);
			if(this.properties != null) {
				if(this.properties.encodeUnsafeChars) {
				  resultIRI = GeneralUtility.encodeUnsafeChars(resultIRI);
				}
				
				if(this.properties.encodeReservedChars) {
					resultIRI = GeneralUtility.encodeReservedChars(resultIRI);
				}
			}
		} catch {
			case e:Exception => {
				logger.warn("Error translating object uri value : " + resultIRI);
			}
		}
		resultIRI	  
	}
	
	def translateLiteral(termMap:R2RMLTermMap, originalLiteral:Object
	    , mapXMLDatatype : Map[String, String]) :String = {
	    var resultLiteral = originalLiteral;
	    try {
			resultLiteral = GeneralUtility.encodeLiteral(resultLiteral.toString());
			if(this.properties != null) {
				if(this.properties.literalRemoveStrangeChars) {
				  resultLiteral = GeneralUtility.removeStrangeChars(resultLiteral.toString());
				}
			}
			
			val datatypeFromMapping = termMap.datatype;
			val language = termMap.languageTag;
				
			val datatype = if(termMap.termMapType == Constants.MorphTermMapType.ColumnTermMap) {
			  if(datatypeFromMapping == null) {
				  val columnName = termMap.columnName;
				  //datatype = mapColumnType.get(columnName);
				  val dbType = this.properties.databaseType;
				  MorphSQLUtility.getXMLDatatype(columnName, mapXMLDatatype, dbType);
				  null
			  } 
			  else { datatypeFromMapping }
			} 
			else { datatypeFromMapping }

			if(datatype != null) {
				val xsdDataTimeURI = XSDDatatype.XSDdateTime.getURI().toString();
				val xsdBooleanURI = XSDDatatype.XSDboolean.getURI().toString();
					  
				datatype match {
					case xsdDataTimeURI => {
						resultLiteral = resultLiteral.toString().trim().replaceAll(" ", "T");
					} 
					case xsdBooleanURI => {
						    val resultLiteralInString = resultLiteral.toString();
							if(resultLiteralInString.equalsIgnoreCase("T") 
							    || resultLiteralInString.equalsIgnoreCase("True") 
							    || resultLiteralInString.equalsIgnoreCase("1")) {
					  			resultLiteral = "true";
					  		} else if(resultLiteralInString.equalsIgnoreCase("F") 
					  		    || resultLiteralInString.equalsIgnoreCase("False") 
					  		    || resultLiteralInString.equalsIgnoreCase("0")) {
					  			resultLiteral = "false";
					  		} else {
					  			resultLiteral = "false";
					  		}

				  	}						    
				  }
			  }
		} catch {
			case e:Exception => {
				logger.warn("Error translating object uri value : " + resultLiteral);
			}
		}
		
	    resultLiteral.toString();	  
	}
	
	def translateData(termMap:R2RMLTermMap, originalValue:Object
	    , mapXMLDatatype : Map[String, String]) = {
		val translatedValue:String = termMap.termType match {
		  case Constants.R2RML_IRI_URI => {
			 this.translateIRI(termMap, originalValue.toString());
		  }
		  case Constants.R2RML_LITERAL_URI => {
			  this.translateLiteral(termMap, originalValue, mapXMLDatatype);
		  }
		  case Constants.R2RML_BLANKNODE_URI => {
		    val resultBlankNode = GeneralUtility.createBlankNode(originalValue.toString());
		    resultBlankNode
		  } 
		  case _ => {
			  originalValue.toString()
		  }
		}
		translatedValue
	}
	
	def translateValue(termMap:R2RMLTermMap, rs:ResultSet , logicalTableAlias:String 
	//    , mapXMLDatatype : Map[String, String]
	) : Object = {
		var originalValue = termMap.getOriginalValue();
		val dbType = this.properties.databaseType;
		val dbEnclosedCharacter = Constants.getEnclosedCharacter(dbType);
		
					  
		val result = termMap.termMapType match {
		  case Constants.MorphTermMapType.ColumnTermMap => {
				if(logicalTableAlias != null && !logicalTableAlias.equals("")) {
					val originalValueSplit = originalValue.split("\\.");
					val columnName = originalValueSplit(originalValueSplit.length - 1).replaceAll("\"", dbEnclosedCharacter);;
					//				originalValue = logicalTableAlias + "." + columnName;
					//val columnNameWithoutEnclosedChar = columnName.replaceAll("\"", "");
					originalValue = logicalTableAlias + "_" + columnName;
				}
				this.getResultSetValue(termMap, rs, originalValue);
			} 
		  case Constants.MorphTermMapType.ConstantTermMap => {
				originalValue;
			} 
		  case Constants.MorphTermMapType.TemplateTermMap => {
				val  attributes = RegexUtility.getTemplateColumns(originalValue, true);
	
				var replacements:Map[String,Object] = Map.empty;
				for(attribute <- attributes) {
					
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
					
					var databaseValue = this.getResultSetValue(termMap, rs, databaseColumn);
	
					if(databaseValue != null) {
						replacements += (attribute -> databaseValue);
					}
				}
				RegexUtility.replaceTokens(originalValue, replacements);
				
			}	
		}


		
		if(result == null) {
			logger.warn("Unfolded value returns NULL!");
		}
		result;
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