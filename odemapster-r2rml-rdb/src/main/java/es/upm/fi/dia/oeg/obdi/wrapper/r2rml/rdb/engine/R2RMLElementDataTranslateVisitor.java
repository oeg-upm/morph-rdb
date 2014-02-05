package es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.engine;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;

import es.upm.fi.dia.oeg.morph.base.Constants;
import es.upm.fi.dia.oeg.morph.base.DatatypeMapper;
import es.upm.fi.dia.oeg.obdi.core.ConfigurationProperties;
import es.upm.fi.dia.oeg.obdi.core.DBUtility;
import es.upm.fi.dia.oeg.obdi.core.ODEMapsterUtility;
import es.upm.fi.dia.oeg.obdi.core.engine.AbstractDataTranslator;
import es.upm.fi.dia.oeg.obdi.core.engine.AbstractUnfolder;
import es.upm.fi.dia.oeg.obdi.core.engine.RDBReader;
import es.upm.fi.dia.oeg.obdi.core.exception.PostProcessorException;
import es.upm.fi.dia.oeg.obdi.core.exception.QueryTranslatorException;
import es.upm.fi.dia.oeg.obdi.core.materializer.AbstractMaterializer;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLGraphMap;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLLogicalTable;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLMappingDocument;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLObjectMap;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLPredicateMap;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLPredicateObjectMap;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLRefObjectMap;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLSubjectMap;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTermMap;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTermMap.TermMapType;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTriplesMap;

public class R2RMLElementDataTranslateVisitor extends AbstractDataTranslator 
implements R2RMLElementVisitor {
	private static Logger logger = Logger.getLogger(R2RMLElementUnfoldVisitor.class);
	
	public R2RMLElementDataTranslateVisitor(
			ConfigurationProperties properties) {
		super(properties);
/*		AbstractUnfolder unfolder = new R2RMLElementUnfoldVisitor();
		String dbType = properties.getDatabaseType();
		unfolder.setDbType(dbType);
		this.setUnfolder(unfolder);*/
	}
	
	public R2RMLElementDataTranslateVisitor(String configurationDirectory
			, String configurationFile) {
		super(configurationDirectory, configurationFile);
/*		AbstractUnfolder unfolder = new R2RMLElementUnfoldVisitor();
		this.setUnfolder(unfolder);*/
	}

	@Override
	protected Object processCustomFunctionTransformationExpression(
			Object argument) throws PostProcessorException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setMaterializer(AbstractMaterializer materializer) {
		this.materializer = materializer;
		
	}

	@Override
	public void translateData(Collection<AbstractConceptMapping> triplesMaps)
			throws Exception {
		for(AbstractConceptMapping triplesMap : triplesMaps) {
			try {
				((R2RMLTriplesMap)triplesMap).accept(this);
			} catch(Exception e) {
				logger.error("error while translating data of triplesMap : " + triplesMap);
				if(e.getMessage() != null) {
					logger.error("error message = " + e.getMessage());
				}
				
				//e.printStackTrace();
				throw new QueryTranslatorException(e.getMessage(), e);
			}
		}
	}
	
	@Override
	public void translateData(AbstractMappingDocument mappingDocument)
			throws Exception {
		Connection conn = this.properties.getConn();
		
		Collection<AbstractConceptMapping> triplesMaps = 
				mappingDocument.getConceptMappings();
		if(triplesMaps != null) {
			this.translateData(triplesMaps);
			DBUtility.closeConnection(conn, R2RMLElementDataTranslateVisitor.class.getName());
		}
		//this.materializer.materialize();
	}

	public void translateObjectMap(R2RMLTermMap objectMap, ResultSet rs
			, Map<String, String> mapColumnType, String subjectGraphName, String predicateobjectGraphName
			, String predicateMapUnfoldedValue, String objectMapUnfoldedValue
			) throws SQLException {

		
		if(objectMap != null && objectMapUnfoldedValue != null) {
			String objectMapTermType = objectMap.getTermType();

			if(Constants.R2RML_IRI_URI().equalsIgnoreCase(objectMapTermType)) {
				try {
					objectMapUnfoldedValue = ODEMapsterUtility.encodeURI(objectMapUnfoldedValue);
				} catch(Exception e) {
					logger.warn("Error encoding object value : " + objectMapUnfoldedValue);
				}					
			}

			if(Constants.R2RML_LITERAL_URI().equalsIgnoreCase(objectMapTermType)) {
				String datatype = objectMap.getDatatype();
				String language = null;
				language = objectMap.getLanguageTag();
				
				if(objectMap.getTermMapType() == TermMapType.COLUMN) {
					if(datatype == null) {
						String columnName = objectMap.getColumnName();
						datatype = mapColumnType.get(columnName);
					}
				}
				
				if(datatype != null) {
					if(XSDDatatype.XSDdateTime.getURI().toString().equals(datatype)) {
						objectMapUnfoldedValue = objectMapUnfoldedValue.trim().replaceAll(" ", "T");
					} else if(XSDDatatype.XSDboolean.getURI().toString().equals(datatype)) {
						if(objectMapUnfoldedValue.equalsIgnoreCase("T") 
								|| objectMapUnfoldedValue.equalsIgnoreCase("True") ) {
							objectMapUnfoldedValue = "true";
						} else if(objectMapUnfoldedValue.equalsIgnoreCase("F") 
								|| objectMapUnfoldedValue.equalsIgnoreCase("Frue")) {
							objectMapUnfoldedValue = "false";
						} else {
							objectMapUnfoldedValue = "false";
						}
					}					
				}
				
				objectMapUnfoldedValue = ODEMapsterUtility.encodeLiteral(objectMapUnfoldedValue);
				if(this.properties != null) {
					if(this.properties.isLiteralRemoveStrangeChars()) {
						objectMapUnfoldedValue = ODEMapsterUtility.removeStrangeChars(objectMapUnfoldedValue);
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
			} else if(Constants.R2RML_IRI_URI().equalsIgnoreCase(objectMapTermType)) {
				try {
					objectMapUnfoldedValue = ODEMapsterUtility.encodeURI(objectMapUnfoldedValue);
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
				} catch(Exception e) {
					
				}
				

			} else if(Constants.R2RML_BLANKNODE_URI().equalsIgnoreCase(objectMapTermType)) {
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
			} else {
				logger.warn("Undefined term type for object map : " + objectMap);
			}

		}
	}

	public Object visit(R2RMLLogicalTable logicalTable) {
		// TODO Auto-generated method stub
		return null;
	}

	public Object visit(R2RMLMappingDocument mappingDocument) throws QueryTranslatorException {
		try {
			this.translateData(mappingDocument);
		} catch (Exception e) {
			logger.error("error during data translation process : " + e.getMessage());
			throw new QueryTranslatorException(e.getMessage());
		}

		return null;
	}

	public Object visit(R2RMLObjectMap objectMap) {
		// TODO Auto-generated method stub
		return null;
	}

	public Object visit(R2RMLRefObjectMap refObjectMap) {
		// TODO Auto-generated method stub
		return null;
	}

	public Object visit(R2RMLTermMap r2rmlTermMap) {
		// TODO Auto-generated method stub
		return null;
	}

	public void generateRDFTriples(R2RMLLogicalTable logicalTable, R2RMLSubjectMap sm
			, Collection<R2RMLPredicateObjectMap> poms, String sqlQuery) 
			throws Exception {
		logger.info("Translating RDB data into RDF instances...");
		Connection conn = this.properties.openConnection();
		int timeout = this.properties.getDatabaseTimeout();
		ResultSet rs = RDBReader.evaluateQuery(sqlQuery, conn, timeout);
		
		Map<String, String> mapXMLDatatype = new HashMap<String, String>();
		Map<String, Integer> mapDBDatatype = new HashMap<String, Integer>();
		ResultSetMetaData rsmd = null;
		DatatypeMapper datatypeMapper = new DatatypeMapper();
		
		try {
			rsmd = rs.getMetaData();
			int columnCount = rsmd.getColumnCount();
			for (int i=0; i<columnCount; i++) {
				String columnName = rsmd.getColumnName(i+1);
				int columnType= rsmd.getColumnType(i+1);
				String mappedDatatype = datatypeMapper.getMappedType(columnType);
				mapXMLDatatype.put(columnName, mappedDatatype);
				mapDBDatatype.put(columnName, new Integer(columnType));
			}
		} catch(Exception e) {
			//e.printStackTrace();
			logger.warn("Unable to detect database columns!");
		}

		int i=0;
		while(rs.next()) {
			i++;
			//translate subject map
			String subjectGraphName = null;
			if(sm != null) {
				R2RMLGraphMap sgm = sm.getGraphMap();
				if(sgm != null) {
					//String subjectGraphAlias = subjectGraph.getAlias();
					subjectGraphName = sgm.getUnfoldedValue(rs, null);
					if(Constants.R2RML_IRI_URI().equalsIgnoreCase(sgm.getTermType())) {
						try {
							subjectGraphName = ODEMapsterUtility.encodeURI(subjectGraphName);
						} catch(Exception e) {
							logger.warn("Error encoding subject graph value : " + subjectGraphName);
						}					
					}
				}
				
				//String logicalTableAlias = subjectMap.getAlias();
				String logicalTableAlias = logicalTable.getAlias();
				
				String subjectValue = sm.getUnfoldedValue(rs, logicalTableAlias);
				if(subjectValue == null) {
					logger.debug("null value in the subject triple!");
				} else {
					if(Constants.R2RML_IRI_URI().equalsIgnoreCase(sm.getTermType())) {
						try {
							subjectValue = ODEMapsterUtility.encodeURI(subjectValue);
						} catch(Exception e) {
							logger.warn("Error encoding subject value : " + subjectValue);
						}
					}

					this.materializer.createSubject(sm.isBlankNode(), subjectValue);

					//rdf:type
					Collection<String> classURIs = sm.getClassURIs();
					if(classURIs != null) {
						for(String classURI : classURIs) {
							this.materializer.materializeRDFTypeTriple(subjectValue, classURI, sm.isBlankNode(), subjectGraphName );
						}				
					}
					
					//translate predicate object map
					if(poms != null) {
						logger.debug("predicateObjectMaps.size() = " + poms.size());
						for(R2RMLPredicateObjectMap predicateObjectMap : poms){
							R2RMLPredicateMap predicateMap = predicateObjectMap.getPredicateMap();
							String predicateMapUnfoldedValue = 
									predicateMap.getUnfoldedValue(rs, null);

							R2RMLGraphMap predicateobjectGraph = predicateObjectMap.getGraphMap();
							String predicateobjectGraphName = null;
							if(predicateobjectGraph != null ) {
								predicateobjectGraphName = 
										predicateobjectGraph.getUnfoldedValue(rs, null);
								if(Constants.R2RML_IRI_URI().equalsIgnoreCase(predicateobjectGraph.getTermType())) {
									try {
										predicateobjectGraphName = ODEMapsterUtility.encodeURI(predicateobjectGraphName);
									} catch(Exception e) {
										logger.warn("Error encoding object graph value : " + predicateobjectGraphName);
									}					
								}
							}

							//translate object map
							R2RMLObjectMap objectMap = predicateObjectMap.getObjectMap();
							if(objectMap != null) {
								//retrieve the alias from predicateObjectMap, not triplesMap!
								String alias = predicateObjectMap.getAlias();
								if(alias == null) {
									alias = logicalTableAlias;
								}
								//String alias = triplesMap.getLogicalTable().getAlias();
								
								String objectMapUnfoldedValue = 
										objectMap.getUnfoldedValue(rs, alias);
								this.translateObjectMap(objectMap, rs, mapXMLDatatype
										, subjectGraphName, predicateobjectGraphName
										, predicateMapUnfoldedValue, objectMapUnfoldedValue
										);
							}

							//translate refobject map
							R2RMLRefObjectMap refObjectMap = predicateObjectMap.getRefObjectMap();
							if(refObjectMap != null) {
								R2RMLElementUnfoldVisitor r2rmlUnfolder = 
										(R2RMLElementUnfoldVisitor) this.unfolder;
//								String joinQueryAlias = refObjectMap.getAlias();
								String joinQueryAlias2 = 
										r2rmlUnfolder.getMapRefObjectMapAlias().get(refObjectMap);
								
								R2RMLSubjectMap parentSubjectMap = 
										refObjectMap.getParentTriplesMap().getSubjectMap();
								//String parentSubjectValue = parentSubjectMap.getUnfoldedValue(rs, refObjectMap.getAlias());
								String parentSubjectValue = parentSubjectMap.getUnfoldedValue(rs, joinQueryAlias2);

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
		
		
		String conceptName = sm.getOwner().getConceptName();
		if(conceptName == null) {
			logger.info(i + " instances retrieved.");
		} else {
			logger.info(i + " instances of " + conceptName + " retrieved.");	
		}
		rs.close();

	}
	
	public Object visit(R2RMLTriplesMap triplesMap) throws Exception {
//		String sqlQuery = triplesMap.accept(
//				new R2RMLElementUnfoldVisitor()).toString();
		R2RMLElementUnfoldVisitor r2rmlUnfolder = (R2RMLElementUnfoldVisitor) this.unfolder;
		String sqlQuery = triplesMap.accept(r2rmlUnfolder).toString();
		this.generateRDFTriples(triplesMap, sqlQuery);
		return null;
	}

	@Override
	public void generateRDFTriples(AbstractConceptMapping cm, String sqlQuery) throws Exception {
		R2RMLTriplesMap triplesMap = (R2RMLTriplesMap) cm;
		R2RMLLogicalTable logicalTable = triplesMap.getLogicalTable();
		R2RMLSubjectMap sm = triplesMap.getSubjectMap();
		Collection<R2RMLPredicateObjectMap> poms = triplesMap.getPredicateObjectMaps();
		this.generateRDFTriples(logicalTable, sm, poms, sqlQuery);
		//conn.close();		
	}

	@Override
	public void generateSubjects(AbstractConceptMapping cm, String sqlQuery) throws Exception {
		R2RMLTriplesMap triplesMap = (R2RMLTriplesMap) cm;
		R2RMLLogicalTable logicalTable = triplesMap.getLogicalTable();
		R2RMLSubjectMap sm = triplesMap.getSubjectMap();
		this.generateRDFTriples(logicalTable, sm, null, sqlQuery);
		//conn.close();		
	}


}
