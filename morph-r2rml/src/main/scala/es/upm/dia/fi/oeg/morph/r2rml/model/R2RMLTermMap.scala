package es.upm.dia.fi.oeg.morph.r2rml.model

import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.ConfigurationProperties
import com.hp.hpl.jena.rdf.model.Resource
import es.upm.fi.dia.oeg.morph.base.RegexUtility
import scala.collection.JavaConversions._
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
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping
import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping
import es.upm.fi.dia.oeg.morph.base.sql.MorphDatabaseMetaData

abstract class R2RMLTermMap
//extends R2RMLElement with IConstantTermMap with  IColumnTermMap with ITemplateTermMap 
extends IConstantTermMap with IColumnTermMap with ITemplateTermMap
{
//	def this(resource:Resource) = {
//	  this();
//	  this.parse(resource);
//	}
//	
//	def this(constantValue:String) = {	
//	  this();
//	}
	
	val logger = Logger.getLogger("R2RMLTermMap");
	var resource:Resource = null;
	
	//var ownerTriplesMap:AbstractConceptMapping=null;
	//var configurationProperties:ConfigurationProperties=null;
	
	var termType:String =null;//IRI, BlankNode, or Literal
	var languageTag:String =null;
	var datatype:String =null;
	var termMapType:Constants.MorphTermMapType.Value =null;

	//for constant type TermMap
	var constantValue:String=null;

	//for column type TermMap
	var columnName:String =null;
	var columnTypeName:String=null;

	//for template type TermMap
	var templateString:String =null;

	//var isNullable = true;

	def buildMetadata(dbMetadata:MorphDatabaseMetaData) {
		
	}

		
	def parse(resource:Resource) {
//		val logicalTable = ownerTriplesMap.getLogicalTable();
//		val tableMetaData:TableMetaData = logicalTable.getTableMetaData();
		//val dbType = this.configurationProperties.databaseType;
//		val dbType = tableMetaData.dbType;
//		val dbEnclosedCharacter = Constants.getEnclosedCharacter(dbType);

		val constantStatement = resource.getProperty(Constants.R2RML_CONSTANT_PROPERTY);
		if(constantStatement != null) {
			this.termMapType = Constants.MorphTermMapType.ConstantTermMap;
			this.constantValue = constantStatement.getObject().toString();
		} else {
			val columnStatement = resource.getProperty(Constants.R2RML_COLUMN_PROPERTY);
			if(columnStatement != null) {
				this.termMapType = Constants.MorphTermMapType.ColumnTermMap;
				this.columnName = columnStatement.getObject().toString();
//				this.columnName = this.columnName.replaceAll("\"", dbEnclosedCharacter);

//				if(tableMetaData != null) {
//					val cmd = if(tableMetaData.getColumnMetaData(this.columnName).isDefined) {
//						tableMetaData.getColumnMetaData(this.columnName).get;
//					} else {
//						null;
//					}
//
//					if(cmd != null) {
//						this.columnTypeName = cmd.dataType;
//					}					
//				}
			} else {
				val templateStatement = resource.getProperty(Constants.R2RML_TEMPLATE_PROPERTY);
				if(templateStatement != null) {
					this.termMapType = Constants.MorphTermMapType.TemplateTermMap;
					this.templateString = templateStatement.getObject().toString();

					val pkColumnStrings : List[String] = this.getTemplateColumns().toList;

//					for(pkColumnString <- pkColumnStrings) {
//						val cmd = if(tableMetaData != null) {
//							val optionColumnMetaData = tableMetaData.getColumnMetaData(pkColumnString);
//							if(optionColumnMetaData.isDefined) {
//								optionColumnMetaData.get;
//							} else {
//								null;
//							}
//						} else {
//							null;							
//						}
//
//						if(cmd != null) {
//							this.columnTypeName = cmd.dataType;
//							if(cmd.isNullable) {
//							}
//						} else {
//							logger.debug("metadata not found for: " + pkColumnString);
//						}
//					}
					//this.isNullable = isNullableAux;
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
	  
		this.resource = resource;
	}
	

//	def this(termMapPosition:Constants.MorphPOS.Value, constantValue:String ) {
//		this();
//		this.termMapType = Constants.MorphTermMapType.ConstantTermMap;
//		this.constantValue = constantValue;
//		this.termType = this.getDefaultTermType();
//	}

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

	def  getReferencedColumns() : List[String] = {
		val result : List[String] = if(this.termMapType == Constants.MorphTermMapType.ColumnTermMap) {
			//List(this.getOriginalValue());
		  List(this.columnName);
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

// Members declared in es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.IColumnTermMap   
	override def getColumnName(): String = this.columnName;   
	override def getColumnTypeName(): String = this.columnTypeName;   
	override def setColumnTypeName(columnTypeName: String) = {
	  this.columnTypeName=columnTypeName;
	}      

// Members declared in es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.IConstantTermMap   
	override def getConstantValue(): String = this.constantValue;   
	override def setConstantValue(constantValue: String): Unit = {
	  this.constantValue=constantValue
	 }

// Members declared in es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.ITemplateTermMap   
	override def getTemplateString(): String = this.templateString; 

}

