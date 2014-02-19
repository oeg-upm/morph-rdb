package es.upm.dia.fi.oeg.morph.r2rml.model

import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.ConfigurationProperties
import com.hp.hpl.jena.rdf.model.Resource
import es.upm.fi.dia.oeg.morph.base.RegexUtility
import scala.collection.JavaConversions._
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLSubjectMap
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLPredicateMap
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLObjectMap
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLGraphMap
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.engine.R2RMLElementVisitor
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype
import java.sql.ResultSet
import es.upm.fi.dia.oeg.morph.base.GeneralUtility
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.ITemplateTermMap
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.ITemplateTermMap
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.IConstantTermMap
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.engine.R2RMLElement
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.IColumnTermMap
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLConstant
import Zql.ZConstant
import java.util.HashMap

abstract class MorphR2RMLTermMap 
//extends R2RMLElement with IConstantTermMap with  IColumnTermMap with ITemplateTermMap 
extends IConstantTermMap with IColumnTermMap with ITemplateTermMap
{
	val logger = Logger.getLogger("R2RMLTermMap");
	
	var owner:R2RMLTriplesMap ;
	var configurationProperties:ConfigurationProperties ;
	
	var termType:String ;//IRI, BlankNode, or Literal
	var languageTag:String ;
	var datatype:String ;
	var termMapType:Constants.MorphTermMapType.Value;

	//for constant type TermMap
	var constantValue:String;

	//for column type TermMap
	var columnName:String ;
	var columnTypeName:String;

	//for template type TermMap
	var templateString:String ;

	//	private boolean isNullable = true;

	def this(resource:Resource , termMapPosition:Constants.MorphPOS.Value
	    , owner:R2RMLTriplesMap ) {
	  this();
	  
		this.configurationProperties = owner.getOwner().getConfigurationProperties();
		this.owner = owner;

		val dbType = this.configurationProperties.databaseType;
		val dbEnclosedCharacter = Constants.getEnclosedCharacter(dbType);
		val logicalTable = owner.getLogicalTable();
		val tableMetaData = logicalTable.getTableMetaData();


		val constantStatement = resource.getProperty(Constants.R2RML_CONSTANT_PROPERTY);
		if(constantStatement != null) {
			this.termMapType = Constants.MorphTermMapType.ConstantTermMap;
			this.constantValue = constantStatement.getObject().toString();
		} else {
			val columnStatement = resource.getProperty(Constants.R2RML_COLUMN_PROPERTY);
			if(columnStatement != null) {
				this.termMapType = Constants.MorphTermMapType.ColumnTermMap;
				this.columnName = columnStatement.getObject().toString();
				this.columnName = this.columnName.replaceAll("\"", dbEnclosedCharacter);

				if(tableMetaData != null) {
					val cmd = if(tableMetaData.getColumnMetaData(this.columnName).isDefined) {
						tableMetaData.getColumnMetaData(this.columnName).get;
					} else {
						null;
					}

					if(cmd != null) {
						this.columnTypeName = cmd.dataType;
					}					
				}
			} else {
				val templateStatement = resource.getProperty(Constants.R2RML_TEMPLATE_PROPERTY);
				if(templateStatement != null) {
					this.termMapType = Constants.MorphTermMapType.TemplateTermMap;
					this.templateString = templateStatement.getObject().toString();

					val pkColumnStrings : List[String] = this.getTemplateColumns().toList;

					for(pkColumnString <- pkColumnStrings) {
						val cmd = if(tableMetaData != null) {
							val optionColumnMetaData = tableMetaData.getColumnMetaData(pkColumnString);
							if(optionColumnMetaData.isDefined) {
								optionColumnMetaData.get;
							} else {
								null;
							}
						} else {
							null;							
						}

						if(cmd != null) {
							this.columnTypeName = cmd.dataType;
							if(cmd.isNullable) {
							}
						} else {
							logger.debug("metadata not found for: " + pkColumnString);
						}
					}
					//					this.isNullable = isNullableAux;
				} else {
					val termMapType = this match {
					  case _:R2RMLSubjectMap => { "SubjectMap"; } 
					  case _:R2RMLPredicateMap => { "PredicateMap"; } 
					  case _:R2RMLObjectMap => { "ObjectMap"; } 
					  case _:R2RMLGraphMap => { "GraphMap"; } 
					  case _ => { "TermMap"; }					  
					}

					val errorMessage = "Invalid " + termMapType + " for " + resource.getLocalName();
					logger.error(errorMessage);
					throw new Exception(errorMessage);
				}
			}
		}

		val datatypeStatement = resource.getProperty(Constants.R2RML_DATATYPE_PROPERTY);
		if(datatypeStatement != null) {
			this.datatype = datatypeStatement.getObject().toString();
		}

		val languageStatement = resource.getProperty(Constants.R2RML_LANGUAGE_PROPERTY);
		if(languageStatement != null) {
			this.languageTag = languageStatement.getObject().toString();
		}

		val termTypeStatement = resource.getProperty(Constants.R2RML_TERMTYPE_PROPERTY);
		if(termTypeStatement == null) {
			this.termType = this.getDefaultTermType();
		} else {
			this.termType = termTypeStatement.getObject().toString();
		}
	  
	}
	

	def this(termMapPosition:Constants.MorphPOS.Value, constantValue:String ) {
		this();
		this.termMapType = Constants.MorphTermMapType.ConstantTermMap;
		this.constantValue = constantValue;
		this.termType = this.getDefaultTermType();
	}

//	def accept(visitor:R2RMLElementVisitor ) : Object = {
//		val result = visitor.visit(this);
//		result;
//	}


	def getDefaultTermType() : String = {
		val result = this match {
		  case _:R2RMLObjectMap => {
			if(this.termMapType == Constants.MorphTermMapType.ColumnTermMap 
			    || this.languageTag != null || this.datatype != null) {
				Constants.R2RML_LITERAL_URI;
			} else { 
				Constants.R2RML_IRI_URI;
			}
		  } 
		  case _ => { Constants.R2RML_IRI_URI;}
		}

		result;
	}

	def  getDatabaseColumnsString() : List[String] = {
		val result : List[String] = if(this.termMapType == Constants.MorphTermMapType.ConstantTermMap) {
			List(this.getOriginalValue());
		} else if(this.termMapType == Constants.MorphTermMapType.TemplateTermMap) {
			val template = this.getOriginalValue();
			RegexUtility.getTemplateColumns(template, true).toList;
		} else {
		  Nil
		}

		result;
	}


	def getOriginalValue() : String = {
	  val result = this.termMapType match {
	    case Constants.MorphTermMapType.ConstantTermMap => { this.constantValue; } 
	    case Constants.MorphTermMapType.ColumnTermMap => { this.columnName; } 
	    case Constants.MorphTermMapType.TemplateTermMap => { this.templateString; } 
	    case _ => { null; }	    
	  }

	  result
	}

	def getResultSetValue(rs:ResultSet, pColumnName:String ) : String = {
		try {
			val dbType = this.configurationProperties.databaseType;
			val zConstant = MorphSQLConstant(pColumnName, ZConstant.COLUMNNAME, dbType);
			val tableName = zConstant.table;
			val columnNameAux = zConstant.column.replaceAll("\"", "");
			val columnName = {
				if(tableName != null) {
					tableName + "." + columnNameAux;
				} else {
				  columnNameAux
				}
			}

			val result = if(this.datatype == null) {
				rs.getString(columnName);
			} else if(this.datatype.equals(XSDDatatype.XSDdateTime.getURI())) {
				rs.getDate(columnName).toString();
			} else {
				rs.getString(columnName);
			}
			result
		} catch {
		  case e:Exception => {
		    //e.printStackTrace();
		    logger.error("error occured when translating result: " + e.getMessage());
		    null
		  }
		}
	}

	def getTemplateColumns() : java.util.List[String] = {
	  val result = this.termMapType match {
	    case Constants.MorphTermMapType.ColumnTermMap => { List(this.columnName); }
	    case Constants.MorphTermMapType.TemplateTermMap => {
				RegexUtility.getTemplateColumns(this.templateString, true).toList;
		}
	    case _ => { Nil }
	  }
	  result;
	}

	def getTemplateValues(uri:String ) : java.util.Map[String, String]  = {
		val result = this.termMapType match {
		  case Constants.MorphTermMapType.TemplateTermMap => {
				val resultAux = RegexUtility.getTemplateMatching(this.templateString, uri);
				resultAux;
			} 
		  case _ => { new HashMap[String, String](); }		  
		}

		result;
	}

	def getUnfoldedValue(rs:ResultSet , logicalTableAlias:String ) : String = {
		
		var originalValue = this.getOriginalValue();

		val result = this.termMapType match {
		  case Constants.MorphTermMapType.ColumnTermMap => {
				if(logicalTableAlias != null && !logicalTableAlias.equals("")) {
					val originalValueSplit = originalValue.split("\\.");
					val columnName = originalValueSplit(originalValueSplit.length - 1);
					//				originalValue = logicalTableAlias + "." + columnName;
					originalValue = logicalTableAlias + "_" + columnName;
				}
				this.getResultSetValue(rs, originalValue);
			} 
		  case Constants.MorphTermMapType.ConstantTermMap => {
				originalValue;
			} 
		  case Constants.MorphTermMapType.TemplateTermMap => {
				val  attributes = RegexUtility.getTemplateColumns(originalValue, true);
	
				var replacements:Map[String,String] = Map.empty;
				for(attribute <- attributes) {
					
					val databaseColumn = if(logicalTableAlias != null) {
						val attributeSplit = attribute.split("\\.");
						if(attributeSplit.length >= 1) {
							logicalTableAlias + "_" + attributeSplit(attributeSplit.length - 1);
						} else {
						  logicalTableAlias + "_" + attribute;
						}
					} else {
						attribute;
					}
					
					var databaseValue = this.getResultSetValue(rs, databaseColumn);
	
					if(databaseValue != null) {
						if(Constants.R2RML_IRI_URI.equals(this.termType)) {
							if(this.configurationProperties.encodeUnsafeChars) {
								databaseValue = GeneralUtility.encodeUnsafeChars(databaseValue);
							}
	
							if(this.configurationProperties.encodeReservedChars) {
								databaseValue = GeneralUtility.encodeReservedChars(databaseValue);
							}							
	
						}
						replacements.put(attribute, databaseValue);
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



	def isBlankNode() : Boolean = {
		if(Constants.R2RML_BLANKNODE_URI.equals(this.termType)) {
			true;
		} else {
			false;
		}
	}

	override def toString() : String = {
		var result = this.termMapType match {
		  case Constants.MorphTermMapType.ConstantTermMap => { "rr:constant"; } 
		  case Constants.MorphTermMapType.ColumnTermMap => { "rr:column"; } 
		  case Constants.MorphTermMapType.TemplateTermMap =>  { "rr:template"; }
		  case _ => "";
		}	

		result += "::" + this.getOriginalValue();

		if(this.termMapType == Constants.MorphTermMapType.ColumnTermMap) {
			if(this.columnTypeName != null) {
				result += ":" + this.columnTypeName;	
			}
		}

		return result;
	}



}

object R2RMLTermMap {
	val logger = Logger.getLogger("R2RMLTermMap");


}