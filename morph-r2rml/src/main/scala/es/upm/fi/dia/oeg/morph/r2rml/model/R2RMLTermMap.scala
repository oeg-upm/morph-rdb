package es.upm.fi.dia.oeg.morph.r2rml.model

import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.Constants
import com.hp.hpl.jena.rdf.model.Resource
import es.upm.fi.dia.oeg.morph.base.RegexUtility
import scala.collection.JavaConversions._
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype
import java.sql.ResultSet
import es.upm.fi.dia.oeg.morph.base.GeneralUtility
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLConstant
import Zql.ZConstant
import java.util.HashMap
import es.upm.fi.dia.oeg.morph.base.sql.MorphDatabaseMetaData
import com.hp.hpl.jena.rdf.model.RDFNode
import es.upm.fi.dia.oeg.morph.r2rml.MorphR2RMLElement
import es.upm.fi.dia.oeg.morph.r2rml.MorphR2RMLElementVisitor

abstract class R2RMLTermMap(val termMapType:Constants.MorphTermMapType.Value
    , termType:Option[String], val datatype:Option[String], val languageTag:Option[String])
    //extends R2RMLElement with IConstantTermMap with  IColumnTermMap with ITemplateTermMap 
extends MorphR2RMLElement with IConstantTermMap with IColumnTermMap with ITemplateTermMap
{
//	def this(termMapType:Constants.MorphTermMapType.Value, termType:Option[String]) = { 
//	  this(termMapType, termType, None, None)
//	}
	
  def this(rdfNode:RDFNode) = {
	  this(R2RMLTermMap.extractTermMapType(rdfNode), R2RMLTermMap.extractTermType(rdfNode)
	      , R2RMLTermMap.extractDatatype(rdfNode), R2RMLTermMap.extractLanguageTag(rdfNode));
	  this.rdfNode = rdfNode;
	  this.parse(rdfNode);
	}
	
	val logger = Logger.getLogger(this.getClass().getName());
	var rdfNode:RDFNode = null;
	
	def accept(visitor:MorphR2RMLElementVisitor) : Object  = {
		val result = visitor.visit(this);
		result;
	}
	
	def parse(rdfNode:RDFNode) = {
	  this.rdfNode = rdfNode;
	  
	  if(rdfNode.isAnon()) {
		  val resourceNode = rdfNode.asResource();
			val constantStatement = resourceNode.getProperty(Constants.R2RML_CONSTANT_PROPERTY);
			if(constantStatement != null) {
				this.constantValue = constantStatement.getObject().toString();
			} else {
				val columnStatement = resourceNode.getProperty(Constants.R2RML_COLUMN_PROPERTY);
				if(columnStatement != null) {
					this.columnName = columnStatement.getObject().toString();
				} else {
					val templateStatement = resourceNode.getProperty(Constants.R2RML_TEMPLATE_PROPERTY);
					if(templateStatement != null) {
						this.templateString = templateStatement.getObject().toString();
					} else {
						val termMapType = this match {
						  case _:R2RMLSubjectMap => { "SubjectMap"; } 
						  case _:R2RMLPredicateMap => { "PredicateMap"; } 
						  case _:R2RMLObjectMap => { "ObjectMap"; } 
						  case _:R2RMLGraphMap => { "GraphMap"; } 
						  case _ => { "TermMap"; }					  
						}
	
						val errorMessage = "Invalid mapping for " + resourceNode.getLocalName();
						logger.error(errorMessage);
						throw new Exception(errorMessage);
					}
				}
			}	    
	  } else {
	       this.constantValue = rdfNode.toString();
	  }


	}

	def inferTermType() : String = {
	  if(this.termType.isDefined) {
	    this.termType.get
	  } else {
	    this.getDefaultTermType
	  }
	}
	
	def getDefaultTermType() : String = {
		val result = this match {
		  case _:R2RMLObjectMap => {
			if(this.termMapType == Constants.MorphTermMapType.ColumnTermMap 
			    || this.languageTag.isDefined || this.datatype.isDefined ) {
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

//		if(this.termMapType == Constants.MorphTermMapType.ColumnTermMap) {
//			if(this.columnTypeName != null) {
//				result += ":" + this.columnTypeName;	
//			}
//		}

		return result;
	}

}

object R2RMLTermMap {
	val logger = Logger.getLogger(this.getClass().getName());
	
	def determineTermMapType(resource:Resource) = {
	  
	}
	
	def extractTermType(rdfNode:RDFNode) = {
	  rdfNode match {
	    case resource:Resource => {
	    	val termTypeStatement = resource.getProperty(Constants.R2RML_TERMTYPE_PROPERTY);
	    	if(termTypeStatement != null) {
	    		Some(termTypeStatement.getObject().toString());  
	    	} else {
	    	  None
	    	}
	    }
	    case _ => {
	      None
	    }
	  }
	  
	}
	
	def extractTermMapType(rdfNode:RDFNode) = {
	  rdfNode match {
	    case resource:Resource => {
			val constantStatement = resource.getProperty(Constants.R2RML_CONSTANT_PROPERTY);
			if(constantStatement != null) {
				Constants.MorphTermMapType.ConstantTermMap;
			} else {
				val columnStatement = resource.getProperty(Constants.R2RML_COLUMN_PROPERTY);
				if(columnStatement != null) {
					Constants.MorphTermMapType.ColumnTermMap;
				} else {
					val templateStatement = resource.getProperty(Constants.R2RML_TEMPLATE_PROPERTY);
					if(templateStatement != null) {
						Constants.MorphTermMapType.TemplateTermMap;
					} else { 
						val errorMessage = "Invalid mapping for " + resource.getLocalName();
						logger.error(errorMessage);
						throw new Exception(errorMessage);
					}
				}
			}	      
	    }
	    case _ => {
	      Constants.MorphTermMapType.ConstantTermMap;
	    }
	  }

	  
	}
	
	
	def extractDatatype(rdfNode:RDFNode) = {
	  rdfNode match {
	    case resource:Resource => {
			val datatypeStatement = resource.getProperty(Constants.R2RML_DATATYPE_PROPERTY);
			if(datatypeStatement != null) {
				Some(datatypeStatement.getObject().toString());
			} else {
			  None
			}	      
	    }
	    case _ => {
	      None
	    }
	  }
	  

	}
	
	def extractLanguageTag(rdfNode:RDFNode) = {
	  rdfNode match {
	    case resource:Resource => {
			val languageStatement = resource.getProperty(Constants.R2RML_LANGUAGE_PROPERTY);
			if(languageStatement != null) {
				Some(languageStatement.getObject().toString());
			} else {
			  None
			}	      
	    }
	    case _ => {
	      None
	    }
	  }

	}
	
	def extractCoreProperties(rdfNode:RDFNode) = {
	  val termMapType=R2RMLTermMap.extractTermMapType(rdfNode);
	  val datatype=R2RMLTermMap.extractDatatype(rdfNode);
	  val languageTag=R2RMLTermMap.extractLanguageTag(rdfNode);
	  val termType=R2RMLTermMap.extractTermType(rdfNode);
	  
	  val coreProperties = (termMapType, termType, datatype, languageTag) 
	  coreProperties;
	}
	
	def extractTermMaps(resource:Resource, termMapType:Constants.MorphPOS.Value) 
	: Set[R2RMLTermMap]= {
		val mapProperties1 = termMapType match {
		  case Constants.MorphPOS.sub => { List(Constants.R2RML_SUBJECTMAP_PROPERTY); }
		  case Constants.MorphPOS.pre => { List(Constants.R2RML_PREDICATEMAP_PROPERTY); }
		  case Constants.MorphPOS.obj => { List(Constants.R2RML_OBJECTMAP_PROPERTY); }
		  case Constants.MorphPOS.graph => { List(Constants.R2RML_GRAPHMAP_PROPERTY); }
		  case _ => { Nil }
		}
		  
		val maps1 = mapProperties1.map(mapProperty => {
			val mapStatements = resource.listProperties(mapProperty);
			if(mapStatements != null) {
				mapStatements.toList().flatMap(mapStatement => {
					if(mapStatement != null) {
						val mapStatementObject = mapStatement.getObject();
						termMapType match {
						  case Constants.MorphPOS.sub => {
							  val sm = R2RMLSubjectMap(mapStatementObject);
							  Some(sm)
						  }
						  case Constants.MorphPOS.pre => {
							  val pm = R2RMLPredicateMap(mapStatementObject);
							  Some(pm);
						  }
						  case Constants.MorphPOS.obj => {
								val mapStatementObjectResource = mapStatementObject.asInstanceOf[Resource];
								if(!R2RMLRefObjectMap.isRefObjectMap(mapStatementObjectResource)) {
									val om = R2RMLObjectMap(mapStatementObject);
									om.parse(mapStatementObject)
									Some(om);
								} else { None; }
						  }
						  case Constants.MorphPOS.graph => {
							  val gm = R2RMLGraphMap(mapStatementObject);
							  if(Constants.R2RML_DEFAULT_GRAPH_URI.equals(gm.getOriginalValue)) {
								  None
							  } else { Some(gm)  }
						  }
						  case _ => { None }
						}
					} else { None }			  
				});
			} else { Nil }
		}).flatten
		
		val mapProperties2 = termMapType match {
		  case Constants.MorphPOS.sub => { List(Constants.R2RML_SUBJECT_PROPERTY); }
		  case Constants.MorphPOS.pre => { List(Constants.R2RML_PREDICATE_PROPERTY); }
		  case Constants.MorphPOS.obj => { List(Constants.R2RML_OBJECT_PROPERTY); }
		  case Constants.MorphPOS.graph => { List(Constants.R2RML_GRAPH_PROPERTY); }
		  case _ => { Nil }
		}
		val maps2 = mapProperties2.map(mapProperty => {
			val mapStatements = resource.listProperties(mapProperty);
			if(mapStatements != null) {
				mapStatements.toList().flatMap(mapStatement => {
					if(mapStatement != null) {
						val mapStatementObject = mapStatement.getObject();
						termMapType match {
							case Constants.MorphPOS.sub => {
								val sm = new R2RMLSubjectMap(Constants.MorphTermMapType.ConstantTermMap
								    , Some(Constants.R2RML_IRI_URI), None, None, Set.empty, Set.empty);
								sm.parse(mapStatementObject)
								Some(sm)
							}
							case Constants.MorphPOS.pre => {
								val pm = new R2RMLPredicateMap(Constants.MorphTermMapType.ConstantTermMap
								    , Some(Constants.R2RML_IRI_URI), None, None);
								pm.parse(mapStatementObject)
								Some(pm);
							}
							case Constants.MorphPOS.obj => {
								val om = new R2RMLObjectMap(Constants.MorphTermMapType.ConstantTermMap
								    , Some(Constants.R2RML_IRI_URI), None, None);
								om.parse(mapStatementObject)
								Some(om)
							}
							case Constants.MorphPOS.graph => {
								val gm = new R2RMLGraphMap(Constants.MorphTermMapType.ConstantTermMap
								    , Some(Constants.R2RML_IRI_URI), None, None);
								gm.parse(mapStatementObject)
								if(Constants.R2RML_DEFAULT_GRAPH_URI.equals(gm.getOriginalValue)) {
									None
								} else { Some(gm) }
							}
							case _ => { None }
						}
					} else { None }			  
				});
			} else { Nil }
		}).flatten
		
		val maps = maps1 ++ maps2
		maps.toSet
	}
	
}