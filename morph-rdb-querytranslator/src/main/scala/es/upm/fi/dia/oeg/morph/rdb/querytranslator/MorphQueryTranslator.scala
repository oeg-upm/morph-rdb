package es.upm.fi.dia.oeg.morph.rdb.querytranslator

import scala.collection.JavaConversions._

import java.sql.Connection;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import Zql.ZConstant;
import Zql.ZExp;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

import es.upm.fi.dia.oeg.morph.base.CollectionUtility;
import es.upm.fi.dia.oeg.morph.base.Constants;
import es.upm.fi.dia.oeg.morph.base.RegexUtility;
import es.upm.fi.dia.oeg.obdi.core.ConfigurationProperties;
import es.upm.fi.dia.oeg.obdi.core.ODEMapsterUtility;
import es.upm.fi.dia.oeg.obdi.core.engine.AbstractResultSet;
import es.upm.fi.dia.oeg.obdi.core.engine.AbstractUnfolder;
import es.upm.fi.dia.oeg.obdi.core.exception.InsatisfiableSQLExpression;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractPropertyMapping;
import es.upm.fi.dia.oeg.obdi.core.querytranslator.AbstractAlphaGenerator;
import es.upm.fi.dia.oeg.obdi.core.querytranslator.AbstractBetaGenerator;
import es.upm.fi.dia.oeg.obdi.core.querytranslator.AbstractCondSQLGenerator;
import es.upm.fi.dia.oeg.obdi.core.querytranslator.AbstractPRSQLGenerator;
import es.upm.fi.dia.oeg.obdi.core.querytranslator.AbstractQueryTranslator;
import es.upm.fi.dia.oeg.obdi.core.querytranslator.QueryTranslationException;
import es.upm.fi.dia.oeg.obdi.core.sql.IQuery;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.R2RMLUtility;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.engine.R2RMLElementUnfoldVisitor;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLMappingDocument;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLRefObjectMap;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTermMap;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTermMap.TermMapType;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTriplesMap;

class MorphQueryTranslator extends AbstractQueryTranslator {
	val logger = Logger.getLogger("MorphQueryTranslator");
	
	var mapTripleAlias:Map[Triple, String] = Map.empty;
	var mapTemplateMatcher:Map[String, Matcher] = Map.empty;
	var mapTemplateAttributes:Map[String, Collection[String]] = Map.empty;
	
	this.unfolder = new R2RMLElementUnfoldVisitor();

	override def transIRI(node:Node) : java.util.List[ZExp] = {
		val cms = mapInferredTypes.get(node);
		val cm = cms.iterator().next().asInstanceOf[R2RMLTriplesMap];
		val mapColumnsValues = cm.getSubjectMap().getTemplateValues(node.getURI());
		val result:List[ZExp] = {
			if(mapColumnsValues == null || mapColumnsValues.size() == 0) {
				//do nothing
			  Nil
			} else {
				val resultAux = mapColumnsValues.keySet().map(column => {
					val value = mapColumnsValues.get(column);
					val constant = new ZConstant(value, ZConstant.UNKNOWN);
					constant;			  
				})
				resultAux.toList;
			}		  
		}

		result;
	}

	override def buildAlphaGenerator() = {
		val alphaGenerator = new MorphAlphaGenerator(this);
		super.setAlphaGenerator(alphaGenerator);
		
	}

	override def buildBetaGenerator() = {
		val betaGenerator = new MorphBetaGenerator(this);
		super.setBetaGenerator(betaGenerator);
	}

	override def buildCondSQLGenerator() = {
		val condSQLGenerator = new MorphCondSQLGenerator(this);
		super.setCondSQLGenerator(condSQLGenerator);
	}

	override def buildPRSQLGenerator() = {
		val prSQLGenerator = new MorphPRSQLGenerator(this);
		super.setPrSQLGenerator(prSQLGenerator);
	}

	override def translateResultSet(varName:String , rs:AbstractResultSet ) : String  = {
		val result:String = {
		try {
			if(rs != null) {
				val rsColumnNames = rs.getColumnNames();
				val columnNames = CollectionUtility.getElementsStartWith(rsColumnNames, varName + "_");

				//Map<String, Object> mapNodeMapping = this.getMapVarMapping2();
				//Map<Integer, Object> mapMappingHashCode = super.getMapHashCodeMapping();
				val mapValue : Object= {
					try {
						val mappingHashCode = rs.getInt(Constants.PREFIX_MAPPING_ID + varName);
						if(mappingHashCode == null) {
							val varNameHashCode = varName.hashCode();
							//mapValue = mapMappingHashCode.get(varNameHashCode);
							super.getMappedMapping(varNameHashCode);
						} else {
							//mapValue = mapMappingHashCode.get(mappingHashCode);
							super.getMappedMapping(mappingHashCode);
						}
					} catch {
					  case e:Exception => {
					    null
					  }
					}				  
				}



				//				if(mapValue == null) {
				//					mapValue = mapNodeMapping.get(varName);	
				//				}

				if(mapValue == null) {
					rs.getString(varName);
				} else {
					val termMap : R2RMLTermMap = {
						mapValue match {
						  case mappedValueTermMap:R2RMLTermMap => {
						    mappedValueTermMap;
						  }
						  case mappedValueRefObjectMap:R2RMLRefObjectMap => {
							mappedValueRefObjectMap.getParentTriplesMap().getSubjectMap();
						  }
						  case _ => {
						    logger.debug("undefined type of mapping!");
						    null
						  }
						}					  
					}


					val resultAux = {
						if(termMap != null) {
							val termMapType = termMap.getTermMapType();
	
							if(termMapType == TermMapType.TEMPLATE) {
								val templateString = termMap.getTemplateString();
								if(this.mapTemplateMatcher.contains(templateString)) {
									val matcher = this.mapTemplateMatcher.get(templateString);  
								} else {
									val pattern = Pattern.compile(Constants.R2RML_TEMPLATE_PATTERN);
									val matcher = pattern.matcher(templateString);
									this.mapTemplateMatcher += (templateString -> matcher);							  
								}
								
								val templateAttributes = {
									if(this.mapTemplateAttributes.contains(templateString)) {
										this.mapTemplateAttributes(templateString);  
									} else {
										val templateAttributesAux = RegexUtility.getTemplateColumns(templateString, true);
										this.mapTemplateAttributes += (templateString -> templateAttributesAux);
										templateAttributesAux;
									}							  
								}
	
								var i = 0;
								val replaceMentAux = templateAttributes.map(templateAttribute => {
									val columnName = {
										if(columnNames == null || columnNames.isEmpty()) {
											varName;
										} else {
											varName + "_" + i;
										}								  
									}
									i = i + 1;
	
									val dbValue = rs.getString(columnName);
									if(dbValue != null) {
										templateAttribute -> dbValue;	
									} else {
										templateAttribute -> dbValue;
									}
								})
								val replacements = replaceMentAux.toMap;
								
								if(replacements.size() > 0) {
									R2RMLUtility.replaceTokens(templateString, replacements);	
								} else {
									logger.debug("no replacements found for the R2RML template!");
									null;
								}
							} else if(termMapType == TermMapType.COLUMN) {
								//String columnName = termMap.getColumnName();
								rs.getString(varName);
							} else if (termMapType == TermMapType.CONSTANT) {
								termMap.getConstantValue();
							} else {
								logger.debug("Unsupported term map type!");
								null;
							}
						} else {
						  null;
						}				  
					}
					

					val resultAuxString = {
						if(resultAux != null) {
							val termMapType = termMap.getTermType();
							if(termMapType != null) {
								if(termMapType.equals(Constants.R2RML_IRI_URI)) {
									ODEMapsterUtility.encodeURI(resultAux);
								} else if(termMapType.equals(Constants.R2RML_LITERAL_URI)) {
									ODEMapsterUtility.encodeLiteral(resultAux);
								} else {
								  resultAux
								}
							} else {
							  resultAux
							}
						} else {
						  null
						}					  
					}
					resultAuxString;
				}
			} else {
			  null
			}
		} catch {
		  case e:Exception => {
		    logger.debug("Error occured while translating result set : " + e.getMessage());
		    null;
		  }
		}		  
		}

		result;
	}

	override def trans(tp:Triple , cm:AbstractConceptMapping ,predicateURI:String 
	    , pm:AbstractPropertyMapping ) : IQuery = {
		// TODO Auto-generated method stub
		null;
	}

	override def getTripleAlias(tp:Triple ) : String = {
	  if(this.mapTripleAlias.contains(tp)) {
	    this.mapTripleAlias(tp);
	  } else {
	    null
	  }
	}

	override def putTripleAlias(tp:Triple , alias:String ) = {
		this.mapTripleAlias += (tp -> alias);
	}
	
}

object MorphQueryTranslator {
	def createQueryTranslator(mappingDocument:AbstractMappingDocument) 
	: AbstractQueryTranslator = {
		val queryTranslator = new MorphQueryTranslator();
		queryTranslator.setMappingDocument(mappingDocument);
		queryTranslator;
	}

	def createQueryTranslator(mappingDocumentPath:String) : AbstractQueryTranslator = {
		MorphQueryTranslator.createQueryTranslator(mappingDocumentPath, null);
	}

	def  createQueryTranslator(mappingDocumentPath:String , conn:Connection ) : AbstractQueryTranslator = {
		val properties = new ConfigurationProperties();
		properties.setConn(conn);
		val mappingDocument = new R2RMLMappingDocument(mappingDocumentPath, properties);
		val queryTranslator = new MorphQueryTranslator();
		queryTranslator.setMappingDocument(mappingDocument);
		queryTranslator;		
	}
}