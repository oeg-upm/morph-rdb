package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import scala.collection.JavaConversions._
import es.upm.fi.dia.oeg.morph.base.ConfigurationProperties
import es.upm.fi.dia.oeg.obdi.core.engine.AbstractDataTranslator
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.obdi.core.materializer.AbstractMaterializer
import java.util.Collection
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.obdi.core.exception.QueryTranslatorException
import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument
import es.upm.fi.dia.oeg.morph.base.DBUtility
import java.sql.ResultSet
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTermMap
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.GeneralUtility
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTermMap.TermMapType
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLLogicalTable
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLObjectMap
import es.upm.fi.dia.oeg.morph.base.MorphSQLUtility
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLSubjectMap
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLPredicateObjectMap
import es.upm.fi.dia.oeg.obdi.core.engine.RDBReader
import es.upm.fi.dia.oeg.morph.base.DatatypeMapper
import java.sql.ResultSetMetaData
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLRefObjectMap
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.engine.R2RMLUnfolder

class R2RMLDataTranslator(properties:ConfigurationProperties) 
extends AbstractDataTranslator(properties:ConfigurationProperties ){
	val logger = Logger.getLogger("R2RMLDataTranslator");

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

	def translateObjectMap(objectMap:R2RMLTermMap, rs:ResultSet
	    , mapColumnType:Map[String, String] , subjectGraphName:String
	    , predicateobjectGraphName:String, predicateMapUnfoldedValue:String
	    ,  pObjectMapUnfoldedValue:String) = {

		var objectMapUnfoldedValue = pObjectMapUnfoldedValue;
		
		if(objectMap != null && pObjectMapUnfoldedValue != null) {
			val objectMapTermType = objectMap.getTermType();

			if(Constants.R2RML_IRI_URI.equalsIgnoreCase(objectMapTermType)) {
				try {
					objectMapUnfoldedValue = GeneralUtility.encodeURI(objectMapUnfoldedValue);
				} catch {
				  case e:Exception => {
				    logger.warn("Error encoding object value : " + objectMapUnfoldedValue);
				  }
				}					
			}

			objectMapTermType match {
			  case Constants.R2RML_LITERAL_URI => {
					val datatypeFromMapping = objectMap.getDatatype();
					val language = objectMap.getLanguageTag();
					
					val datatype = if(objectMap.getTermMapType() == TermMapType.COLUMN) {
						if(datatypeFromMapping == null) {
							val columnName = objectMap.getColumnName();
							//datatype = mapColumnType.get(columnName);
							this.getXMLDatatype(columnName, mapColumnType);
						} else {
						  datatypeFromMapping
						}
					} else {
					  datatypeFromMapping
					}
					
					if(datatype != null) {
					  val xsdDataTimeURI = XSDDatatype.XSDdateTime.getURI().toString();
					  val xsdBooleanURI = XSDDatatype.XSDboolean.getURI().toString();
					  
					  datatype match {
					    case xsdDataTimeURI => {
							objectMapUnfoldedValue = objectMapUnfoldedValue.trim().replaceAll(" ", "T");
						} 
					    case xsdBooleanURI => {
							if(objectMapUnfoldedValue.equalsIgnoreCase("T") 
									|| objectMapUnfoldedValue.equalsIgnoreCase("True")
									|| objectMapUnfoldedValue.equalsIgnoreCase("1")) {
								objectMapUnfoldedValue = "true";
							} else if(objectMapUnfoldedValue.equalsIgnoreCase("F") 
									|| objectMapUnfoldedValue.equalsIgnoreCase("False")
									|| objectMapUnfoldedValue.equalsIgnoreCase("0")) {
								objectMapUnfoldedValue = "false";
							} else {
								objectMapUnfoldedValue = "false";
							}
						}						    
					  }
					}
					
					objectMapUnfoldedValue = GeneralUtility.encodeLiteral(objectMapUnfoldedValue);
					if(this.properties != null) {
						if(this.properties.literalRemoveStrangeChars) {
							objectMapUnfoldedValue = GeneralUtility.removeStrangeChars(objectMapUnfoldedValue);
						}
					}
					
					if(subjectGraphName == null && predicateobjectGraphName == null) {
						this.materializer.materializeDataPropertyTriple(predicateMapUnfoldedValue
								, objectMapUnfoldedValue, datatype, language, null );
					} else {
						if(subjectGraphName != null) {
							this.materializer.materializeDataPropertyTriple(
									predicateMapUnfoldedValue, objectMapUnfoldedValue
									, datatype, language, subjectGraphName );
						}
						
						if(predicateobjectGraphName != null) {
							if(subjectGraphName == null || 
									!predicateobjectGraphName.equals(subjectGraphName)) {
								this.materializer.materializeDataPropertyTriple(
										predicateMapUnfoldedValue, objectMapUnfoldedValue
										, datatype, language, predicateobjectGraphName);							
							}
						}
					}
				} 
			  case Constants.R2RML_IRI_URI => {
					try {
						objectMapUnfoldedValue = GeneralUtility.encodeURI(objectMapUnfoldedValue);
						if(subjectGraphName == null && predicateobjectGraphName == null) {
							this.materializer.materializeObjectPropertyTriple(predicateMapUnfoldedValue, objectMapUnfoldedValue, false, null );
						} else {
							if(subjectGraphName != null) {
								this.materializer.materializeObjectPropertyTriple(predicateMapUnfoldedValue, objectMapUnfoldedValue, false, subjectGraphName );
							}
							if(predicateobjectGraphName != null) {
								if(subjectGraphName == null || 
										!predicateobjectGraphName.equals(subjectGraphName)) {
									this.materializer.materializeObjectPropertyTriple(predicateMapUnfoldedValue, objectMapUnfoldedValue, false, predicateobjectGraphName );
								}
							}
						}					
					} catch {
					  case e:Exception => {}
						
					}
					
	
				} 
			  case Constants.R2RML_BLANKNODE_URI => {
					if(subjectGraphName == null && predicateobjectGraphName == null) {
						this.materializer.materializeObjectPropertyTriple(predicateMapUnfoldedValue, objectMapUnfoldedValue, true, null );
					} else {
						if(subjectGraphName != null) {
							this.materializer.materializeObjectPropertyTriple(predicateMapUnfoldedValue, objectMapUnfoldedValue, true, subjectGraphName );
						}
						if(predicateobjectGraphName != null) {
							if(subjectGraphName == null || 
									!predicateobjectGraphName.equals(subjectGraphName)) {
								this.materializer.materializeObjectPropertyTriple(predicateMapUnfoldedValue, objectMapUnfoldedValue, true, predicateobjectGraphName );
							}
							
						}
					}					
				} 
			  case _ => {
					logger.warn("Undefined term type for object map : " + objectMap);
				}			  
			}


		}
	}

	def visit( logicalTable:R2RMLLogicalTable) : Object = {
		// TODO Auto-generated method stub
		null;
	}

	def visit(mappingDocument:R2RMLMappingDocument) : Object = {
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

	def getXMLDatatype(pColumnName:String, mapXMLDatatype:Map[String, String]) : String = {
		val dbType = this.properties.databaseType;
		val columnName = MorphSQLUtility.printWithoutEnclosedCharacters(pColumnName, dbType);
		val mappedType = mapXMLDatatype.find(p => p._1.equalsIgnoreCase(columnName));
		val result = if(mappedType.isDefined) { mappedType.get._2 } else { null }
		result;
	}
	
	def generateRDFTriples(logicalTable:R2RMLLogicalTable ,  sm:R2RMLSubjectMap
			, poms:Collection[R2RMLPredicateObjectMap] , sqlQuery:String ) = {
		logger.info("Translating RDB data into RDF instances...");
		val conn = if(this.properties.conn == null) {
			DBUtility.getLocalConnection(this.properties.databaseUser
					, this.properties.databaseName, this.properties.databasePassword
					, this.properties.databaseDriver, this.properties.databaseURL, 
					"R2RMLDataTranslator");
		} else {
		  this.properties.conn
		}
		
		val timeout = this.properties.databaseTimeout;
		val rs = RDBReader.evaluateQuery(sqlQuery, conn, timeout);
		
		var  mapXMLDatatype : Map[String, String] = Map.empty;
		var mapDBDatatype:Map[String, Integer]  = Map.empty;
		var rsmd : ResultSetMetaData = null;
		val datatypeMapper = new DatatypeMapper();
		
		try {
			rsmd = rs.getMetaData();
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

		var i=0;
		while(rs.next()) {
			try {
				//translate subject map
				
				if(sm != null) {
				  
					val logicalTableAlias = logicalTable.getAlias();

					val sgm = sm.getGraphMap();
					val subjectGraphName = if(sgm != null) {
						//String subjectGraphAlias = subjectGraph.getAlias();
						var subjectGraphNameAux = sgm.getUnfoldedValue(rs, logicalTableAlias);
						val graphMapTermType = sgm.getTermType();
						graphMapTermType match {
						  case Constants.R2RML_IRI_URI => {
								try {
									subjectGraphNameAux = GeneralUtility.encodeURI(subjectGraphNameAux);
								} catch{
								  case e:Exception => {
								    logger.warn("Error encoding subject graph value : " + subjectGraphNameAux);
								  }
								}					
							} 
						  case Constants.R2RML_LITERAL_URI => {
								val errorMessage = "graph value is not a valid URI: " + subjectGraphNameAux;
								logger.warn(errorMessage);
								throw new Exception(errorMessage);
							}						  
						}
						subjectGraphNameAux
					} else {
					  null
					}
					
					//String logicalTableAlias = subjectMap.getAlias();
					
					val subjectValueAux = sm.getUnfoldedValue(rs, logicalTableAlias);
					if(subjectValueAux == null) {
						logger.debug("null value in the subject triple!");
					} else {
						val subjectValue = 
							try {
								GeneralUtility.encodeURI(subjectValueAux);
							} catch {
							  case e:Exception => {
								logger.warn("Error encoding subject value : " + subjectValueAux);
								subjectValueAux							    
							  }
							}

						this.materializer.createSubject(sm.isBlankNode(), subjectValue);

						//rdf:type
						val classURIs = sm.getClassURIs();
						if(classURIs != null) {
							for(classURI <- classURIs) {
								this.materializer.materializeRDFTypeTriple(subjectValue, classURI, sm.isBlankNode(), subjectGraphName );
							}				
						}
						
						//translate predicate object map
						if(poms != null) {
							logger.debug("predicateObjectMaps.size() = " + poms.size());
							for(predicateObjectMap <- poms){
								val predicateMaps = predicateObjectMap.getPredicateMaps();
								if(predicateMaps != null && !predicateMaps.isEmpty()) {
									for(predicateMap <- predicateMaps) {
//										R2RMLPredicateMap predicateMap = 
//												predicateObjectMap.getPredicateMap(0);
										val predicateMapUnfoldedValue = 
												predicateMap.getUnfoldedValue(rs, null);

										val predicateobjectGraph = predicateObjectMap.getGraphMap();
										var predicateobjectGraphName : String = null;
										if(predicateobjectGraph != null ) {
											predicateobjectGraphName = 
													predicateobjectGraph.getUnfoldedValue(rs, null);
											if(Constants.R2RML_IRI_URI.equalsIgnoreCase(predicateobjectGraph.getTermType())) {
												try {
													predicateobjectGraphName = GeneralUtility.encodeURI(predicateobjectGraphName);
												} catch {
												  case e:Exception => {
												    logger.warn("Error encoding object graph value : " + predicateobjectGraphName);
												  }
												}					
											}
										}

										//translate object map
										val objectMaps = predicateObjectMap.getObjectMaps();
										if(objectMaps != null && !objectMaps.isEmpty()) {
											for(objectMap <- objectMaps) {
												//R2RMLObjectMap objectMap = predicateObjectMap.getObjectMap(0);
												if(objectMap != null) {
													//retrieve the alias from predicateObjectMap, not triplesMap!
													val alias = if(predicateObjectMap.getAlias() == null) {
														logicalTableAlias;
													} else {
													  predicateObjectMap.getAlias()
													}
													//String alias = triplesMap.getLogicalTable().getAlias();
													
													val objectMapUnfoldedValue = 
															objectMap.getUnfoldedValue(rs, alias);
													this.translateObjectMap(objectMap, rs, mapXMLDatatype
															, subjectGraphName, predicateobjectGraphName
															, predicateMapUnfoldedValue, objectMapUnfoldedValue
															);
												}												
											}
										}


										//translate refobject map
										val refObjectMaps = predicateObjectMap.getRefObjectMaps();
										if(refObjectMaps != null && !refObjectMaps.isEmpty()) {
											for(refObjectMap <- refObjectMaps) {
//												R2RMLRefObjectMap refObjectMap = 
//														predicateObjectMap.getRefObjectMap(0);
												if(refObjectMap != null) {
													val r2rmlUnfolder = this.unfolder.asInstanceOf[R2RMLUnfolder];
//													String joinQueryAlias = refObjectMap.getAlias();
													val joinQueryAlias2 = 
															r2rmlUnfolder.getMapRefObjectMapAlias().get(refObjectMap);
													
													val parentSubjectMap = 
															refObjectMap.getParentTriplesMap().getSubjectMap();
													//String parentSubjectValue = parentSubjectMap.getUnfoldedValue(rs, refObjectMap.getAlias());
													val parentSubjectValue = parentSubjectMap.getUnfoldedValue(rs, joinQueryAlias2);

													if(parentSubjectValue != null) {
														this.translateObjectMap(parentSubjectMap, rs, mapXMLDatatype, subjectGraphName
																, predicateobjectGraphName, predicateMapUnfoldedValue, parentSubjectValue
																);
													}
												}												
											}
										}

									}									
								}

							}							
						}
					}
				}
				i = i+1;				
			} catch {
			  case e:Exception => {
			    logger.error(e.getMessage());
			  }
			}
		}
		
		
		val conceptName = sm.getOwner().getConceptName();
		if(conceptName == null) {
			logger.info(i + " instances retrieved.");
		} else {
			logger.info(i + " instances of " + conceptName + " retrieved.");	
		}
		rs.close();

	}
	
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
		val logicalTable = triplesMap.getLogicalTable();
		val sm = triplesMap.getSubjectMap();
		val poms = triplesMap.getPredicateObjectMaps();
		this.generateRDFTriples(logicalTable, sm, poms, sqlQuery);
		//conn.close();		
	}

	override def generateSubjects(cm:AbstractConceptMapping, sqlQuery:String) = {
		val triplesMap = cm.asInstanceOf[R2RMLTriplesMap];
		val logicalTable = triplesMap.getLogicalTable();
		val sm = triplesMap.getSubjectMap();
		this.generateRDFTriples(logicalTable, sm, null, sqlQuery);
		//conn.close();		
	}
	

}