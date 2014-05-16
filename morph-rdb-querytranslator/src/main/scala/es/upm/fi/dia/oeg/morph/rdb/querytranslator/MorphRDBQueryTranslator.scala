package es.upm.fi.dia.oeg.morph.rdb.querytranslator

import scala.collection.JavaConversions._
import java.sql.Connection
import java.util.regex.Matcher
import java.util.regex.Pattern
import org.apache.log4j.Logger
import Zql.ZConstant
import Zql.ZExp
import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.graph.Triple
import es.upm.fi.dia.oeg.morph.base.CollectionUtility
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.RegexUtility
import es.upm.fi.dia.oeg.morph.base.TermMapResult
import es.upm.fi.dia.oeg.morph.base.GeneralUtility
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLTermMap
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLRefObjectMap
import es.upm.fi.dia.oeg.morph.base.querytranslator.NameGenerator
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseBetaGenerator
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseCondSQLGenerator
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseAlphaGenerator
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBasePRSQLGenerator
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.model.MorphBasePropertyMapping
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseResultSet
import es.upm.fi.dia.oeg.morph.base.sql.IQuery
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder

class MorphRDBQueryTranslator(nameGenerator:NameGenerator
    , alphaGenerator:MorphBaseAlphaGenerator, betaGenerator:MorphBaseBetaGenerator
    , condSQLGenerator:MorphBaseCondSQLGenerator, prSQLGenerator:MorphBasePRSQLGenerator) 
    extends MorphBaseQueryTranslator(nameGenerator:NameGenerator
    , alphaGenerator:MorphBaseAlphaGenerator, betaGenerator:MorphBaseBetaGenerator
    , condSQLGenerator:MorphBaseCondSQLGenerator, prSQLGenerator:MorphBasePRSQLGenerator) {

	override val logger = Logger.getLogger("MorphQueryTranslator");
	this.alphaGenerator.owner = this;
	this.betaGenerator.owner = this;
	
	var mapTemplateMatcher:Map[String, Matcher] = Map.empty;
	var mapTemplateAttributes:Map[String, java.util.List[String]] = Map.empty;

	override def transIRI(node:Node) : List[ZExp] = {
		val cms = mapInferredTypes(node);
		val cm = cms.iterator().next().asInstanceOf[R2RMLTriplesMap];
		val mapColumnsValues = cm.subjectMap.getTemplateValues(node.getURI());
		val result:List[ZExp] = {
			if(mapColumnsValues == null || mapColumnsValues.size() == 0) {
				//do nothing
			  Nil
			} else {
				val resultAux = mapColumnsValues.keySet.map(column => {
					val value = mapColumnsValues(column);
					val constant = new ZConstant(value, ZConstant.UNKNOWN);
					constant;			  
				})
				resultAux.toList;
			}		  
		}

		result;
	}

//	override def buildAlphaGenerator() = {
//		val alphaGenerator = new MorphRDBAlphaGenerator(this);
//		super.setAlphaGenerator(alphaGenerator);
//		
//	}
//
//	override def buildBetaGenerator() = {
//		val betaGenerator = new MorphRDBBetaGenerator(this);
//		super.setBetaGenerator(betaGenerator);
//	}
//
//	override def buildCondSQLGenerator() = {
//		val condSQLGenerator = new MorphRDBCondSQLGenerator(this);
//		super.setCondSQLGenerator(condSQLGenerator);
//	}
//
//	override def buildPRSQLGenerator() = {
//		val prSQLGenerator = new MorphRDBPRSQLGenerator(this);
//		super.setPrSQLGenerator(prSQLGenerator);
//	}

	def getMappedMappingByVarName(varName:String, rs:MorphBaseResultSet) = {
		val mapValue = {
			try {
				val mappingHashCode = rs.getInt(Constants.PREFIX_MAPPING_ID + varName);
				
				//IN CASE OF UNION, A VARIABLE MAY MAPPED TO MULTIPLE MAPPINGS
				if(mappingHashCode == null) {
					val varNameHashCode = varName.hashCode();
					//super.getMappedMapping(varNameHashCode);
					this.prSQLGenerator.getMappedMapping(varNameHashCode)
				} else {
					//super.getMappedMapping(mappingHashCode);
					this.prSQLGenerator.getMappedMapping(mappingHashCode)
				}
			} catch {
			  case e:Exception => {
			    null
			  }
			}				  
		}

		mapValue;
	}
	
	override def translateResultSet(varName:String , rs:MorphBaseResultSet ) : TermMapResult  = {
		val result:TermMapResult = {
		try {
			if(rs != null) {
				val rsColumnNames = rs.getColumnNames();
				val columnNames = CollectionUtility.getElementsStartWith(rsColumnNames, varName + "_");
				//val columnNames = CollectionUtility.getElementsStartWith(rsColumnNames, varName);

				val mapValue = this.getMappedMappingByVarName(varName, rs);
				
				if(!mapValue.isDefined) {
					val originalValue = rs.getString(varName);
					new TermMapResult(originalValue, null,None)
				} else {
					val termMap : R2RMLTermMap = {
						mapValue.get match {
						  case mappedValueTermMap:R2RMLTermMap => {
						    mappedValueTermMap;
						  }
						  case mappedValueRefObjectMap:R2RMLRefObjectMap => {
//						    val parentTriplesMap = mappedValueRefObjectMap.getParentTriplesMap().asInstanceOf[R2RMLTriplesMap];
							val md = this.mappingDocument.asInstanceOf[R2RMLMappingDocument];
							val parentTriplesMap = md.getParentTriplesMap(mappedValueRefObjectMap);						    
							parentTriplesMap.subjectMap;
						  }
						  case _ => {
						    logger.debug("undefined type of mapping!");
						    null
						  }
						}					  
					}


					val resultAux = {
						if(termMap != null) {
							val termMapType = termMap.termMapType;
							termMap.termMapType match {
							  case Constants.MorphTermMapType.TemplateTermMap => {
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
										templateAttribute -> dbValue;
									})
									val replacements = replaceMentAux.toMap;
									
									if(replacements.size() > 0) {
										RegexUtility.replaceTokens(templateString, replacements);	
									} else {
										logger.debug("no replacements found for the R2RML template!");
										null;
									}
								} 
							  case Constants.MorphTermMapType.ColumnTermMap => {
									//String columnName = termMap.getColumnName();
									rs.getString(varName);
								} 
							  case Constants.MorphTermMapType.ConstantTermMap => {
									termMap.getConstantValue();
								} 
							  case _ => {
									logger.debug("Unsupported term map type!");
									null;
								}							  
							}
						} else {
						  null;
						}				  
					}
					
					val termMapType = termMap.inferTermType;
					val xsdDatatype = termMap.datatype;
					val resultAuxString = {
						if(resultAux != null) {
							if(termMapType != null) {
								if(termMapType.equals(Constants.R2RML_IRI_URI)) {
									GeneralUtility.encodeURI(resultAux, properties.mapURIEncodingChars);
								} else if(termMapType.equals(Constants.R2RML_LITERAL_URI)) {
									GeneralUtility.encodeLiteral(resultAux);
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
					new TermMapResult(resultAuxString, termMapType, xsdDatatype);
					//resultAuxString;
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

//	override def transTP(tp:Triple , cm:MorphBaseClassMapping ,predicateURI:String 
//	    , pm:MorphBasePropertyMapping ) : IQuery = {
//		// TODO Auto-generated method stub
//		null;
//	}

//	override def getTripleAlias(tp:Triple ) : String = {
//	  if(this.mapTripleAlias.contains(tp)) {
//	    this.mapTripleAlias(tp);
//	  } else {
//	    null
//	  }
//	}
//
//	override def putTripleAlias(tp:Triple , alias:String ) = {
//		this.mapTripleAlias += (tp -> alias);
//	}
	
	   
	   
	//def getMappingDocument(): es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument = ???   
	//def getOptimizer(): es.upm.fi.dia.oeg.obdi.core.engine.IQueryTranslationOptimizer = ???   
	   
	   
	   
	//def setConnection(x$1: java.sql.Connection): Unit = ???   
	   
	//def setIgnoreRDFTypeStatement(x$1: Boolean): Unit = ???   
	   
	   
	//def setUnfolder(x$1: es.upm.fi.dia.oeg.obdi.core.engine.AbstractUnfolder): Unit = ???	
}
//}