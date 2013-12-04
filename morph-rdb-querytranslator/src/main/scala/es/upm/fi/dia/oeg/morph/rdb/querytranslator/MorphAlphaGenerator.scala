package es.upm.fi.dia.oeg.morph.rdb.querytranslator

import scala.collection.JavaConversions._
import es.upm.fi.dia.oeg.obdi.core.querytranslator.AbstractAlphaGenerator
import es.upm.fi.dia.oeg.obdi.core.querytranslator.AbstractQueryTranslator
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping
import es.upm.fi.dia.oeg.obdi.core.querytranslator.AlphaResult
import es.upm.fi.dia.oeg.obdi.core.model.AbstractPropertyMapping
import es.upm.fi.dia.oeg.obdi.core.sql.SQLLogicalTable
import es.upm.fi.dia.oeg.obdi.core.sql.SQLJoinTable
import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.vocabulary.RDF
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLPredicateObjectMap
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.engine.R2RMLElementUnfoldVisitor
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.R2RMLUtility
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.base.DBUtility
import es.upm.fi.dia.oeg.obdi.core.sql.SQLFromItem
import es.upm.fi.dia.oeg.obdi.core.sql.SQLFromItem.LogicalTableType;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLQuery

class MorphAlphaGenerator(owner:AbstractQueryTranslator) 
extends AbstractAlphaGenerator(owner:AbstractQueryTranslator) {
	val logger = Logger.getLogger("MorphQueryTranslator");

	
	override def calculateAlpha(tp:Triple, abstractConceptMapping:AbstractConceptMapping 
	    , predicateURI:String ) : AlphaResult = {
		//alpha subject
		val tpSubject = tp.getSubject();
		val alphaSubject = this.calculateAlphaSubject(tpSubject, abstractConceptMapping);
		val logicalTableAlias = alphaSubject.getAlias();

		
		val pms = abstractConceptMapping.getPropertyMappings(predicateURI);
		val alphaResult : AlphaResult = {
			if(RDF.`type`.getURI().equalsIgnoreCase(predicateURI)) {
				new AlphaResult(alphaSubject, null, predicateURI);
			} else {
				if(pms != null && !pms.isEmpty()) {
					
					//alpha predicate object
					val alphaPredicateObjects:List[SQLJoinTable] = {
						if(pms != null && !pms.isEmpty()) {
							if(pms.size() > 1) {
								val errorMessage = "Multiple mappings of a predicate is not supported.";
								logger.error(errorMessage);
							}
							
							val pm = pms.iterator().next().asInstanceOf[R2RMLPredicateObjectMap];
							val refObjectMap = pm.getRefObjectMap();
							if(refObjectMap != null) { 
								val alphaPredicateObject = this.calculateAlphaPredicateObject(
										tp, abstractConceptMapping, pm, logicalTableAlias);
								List(alphaPredicateObject);
		
	//							val alphaPredicateObject2 = this.calculateAlphaPredicateObject2(
	//									tp, abstractConceptMapping, pm, logicalTableAlias);
	//							alphaPredicateObjects2 = alphaPredicateObjects2 ::: List(alphaPredicateObject2);
							} else {
							  Nil;
							}
						} else {
						  Nil;
						}
					}
					
					new AlphaResult(alphaSubject, alphaPredicateObjects, predicateURI);
					//alphaResult.setAlphaPredicateObjects2(alphaPredicateObjects2);
				} else {
				  null
				}
			}		  
		}

		alphaResult;
	} 

	override def calculateAlpha(tp:Triple, abstractConceptMapping:AbstractConceptMapping 
	    , predicateURI:String , pm:AbstractPropertyMapping ) : AlphaResult = {
	  null;
	} 

	override def calculateAlphaPredicateObject(triple:Triple
	    , abstractConceptMapping:AbstractConceptMapping , abstractPropertyMapping:AbstractPropertyMapping  
		, logicalTableAlias:String ) : SQLJoinTable = {
		
		
		val pm = abstractPropertyMapping.asInstanceOf[R2RMLPredicateObjectMap];  
		val refObjectMap = pm.getRefObjectMap();
		
		val result:SQLJoinTable  =  {
			if(refObjectMap != null) { 
				val parentLogicalTable = refObjectMap.getParentLogicalTable();
				if(parentLogicalTable == null) {
					val errorMessage = "Parent logical table is not found for RefObjectMap : " + refObjectMap;
					logger.error(errorMessage);
				}
				
				val unfolder = this.owner.getUnfolder().asInstanceOf[R2RMLElementUnfoldVisitor];
				val sqlParentLogicalTableAux = unfolder.visit(parentLogicalTable);
				val sqlParentLogicalTable = new SQLJoinTable(sqlParentLogicalTableAux
				    , Constants.JOINS_TYPE_INNER, null);
				
				val tripleAlias = this.owner.getTripleAlias(triple);
				
				val joinQueryAlias = {
					if(tripleAlias == null) {
						val sqlParentLogicalTableAuxAlias = sqlParentLogicalTableAux.generateAlias();
						this.owner.putTripleAlias(triple, sqlParentLogicalTableAuxAlias);
						sqlParentLogicalTableAuxAlias;
					} else {
					  tripleAlias
					}				  
				}

				sqlParentLogicalTableAux.setAlias(joinQueryAlias);
	
				val joinConditions = refObjectMap.getJoinConditions();
				val databaseType = this.owner.getDatabaseType();
				val onExpression = R2RMLUtility.generateJoinCondition(
						joinConditions, logicalTableAlias, joinQueryAlias
						, databaseType);
				if(onExpression != null) {
					sqlParentLogicalTable.setOnExpression(onExpression);
				}
				
				sqlParentLogicalTable;
			} else {
			  null
			}		  
		}
		
		result;
	}
	
	override def calculateAlphaSubject(subject:Node, abstractConceptMapping:AbstractConceptMapping ) 
		: SQLLogicalTable = {
		val cm = abstractConceptMapping.asInstanceOf[R2RMLTriplesMap];
		val r2rmlLogicalTable = cm.getLogicalTable();
		//SQLLogicalTable sqlLogicalTable = new R2RMLElementUnfoldVisitor().visit(logicalTable);
		val unfolder = this.owner.getUnfolder().asInstanceOf[R2RMLElementUnfoldVisitor];
		val sqlLogicalTable = unfolder.visit(r2rmlLogicalTable);

		
		//String logicalTableAlias = sqlLogicalTable.generateAlias();
		val cmLogicalTableAlias = cm.getLogicalTable().getAlias();;
		val logicalTableAlias = {
			if(cmLogicalTableAlias == null || cmLogicalTableAlias.equals("")) {
				sqlLogicalTable.generateAlias();
			} else {
			  cmLogicalTableAlias
			}		  
		}

		//r2rmlLogicalTable.setAlias(logicalTableAlias);
		sqlLogicalTable.setAlias(logicalTableAlias);
		sqlLogicalTable.setDbType(this.owner.getDatabaseType());
//		cm.setAlias(logicalTableAlias);
		return sqlLogicalTable;
	}
	
	override def calculateAlphaPredicateObjectSTG(tp:Triple ,cm:AbstractConceptMapping 
	    , tpPredicateURI:String , logicalTableAlias:String ) : java.util.List[SQLJoinTable] = {
		
		
		val isRDFTypeStatement = RDF.`type`.getURI().equals(tpPredicateURI);
		val  alphaPredicateObjects:List[SQLJoinTable] = {
			if(this.ignoreRDFTypeStatement && isRDFTypeStatement) {
				//do nothing
			  Nil;
			} else {
				val pms = cm.getPropertyMappings(tpPredicateURI);
				if(pms != null && !pms.isEmpty()) {
					val pm = pms.iterator().next().asInstanceOf[R2RMLPredicateObjectMap];
					val refObjectMap = pm.getRefObjectMap();
					if(refObjectMap != null) { 
						val alphaPredicateObject = 
								this.calculateAlphaPredicateObject(tp, cm, pm, logicalTableAlias);
						List(alphaPredicateObject);
					} else {
					  Nil;
					}
				} else {
					if(!isRDFTypeStatement) {
						val errorMessage = "Undefined mapping for : " + tpPredicateURI + " in : " + cm.toString();
						logger.error(errorMessage);				
						Nil;
					}  else {
					  Nil;
					}
				}
			}		  
		}

		alphaPredicateObjects;
	}

	override def calculateAlphaPredicateObjectSTG2(tp:Triple , cm:AbstractConceptMapping 
	    , tpPredicateURI:String , logicalTableAlias:String ) : java.util.List[SQLLogicalTable] = {
		
		val isRDFTypeStatement = RDF.`type`.getURI().equals(tpPredicateURI);
		
		val alphaPredicateObjects : List[SQLLogicalTable] = {
			if(this.ignoreRDFTypeStatement && isRDFTypeStatement) {
				//do nothing
			  Nil;
			} else {
				val pms = cm.getPropertyMappings(tpPredicateURI);
				if(pms != null && !pms.isEmpty()) {
					val pm = pms.iterator().next().asInstanceOf[R2RMLPredicateObjectMap];
					val refObjectMap = pm.getRefObjectMap();
					if(refObjectMap != null) { 
						val alphaPredicateObject = 
								this.calculateAlphaPredicateObject2(tp, cm, pm, logicalTableAlias);
						List(alphaPredicateObject);
					} else {
					  Nil;
					}
				} else {
					if(!isRDFTypeStatement) {
						val errorMessage = "Undefined mapping for : " + tpPredicateURI + " in : " + cm.toString();
						logger.error(errorMessage);
						Nil;
					} else {
					  Nil;
					}
				}
			}		  
		}

		alphaPredicateObjects;
	}
	
	override def calculateAlphaPredicateObject2(triple:Triple 
	    , abstractConceptMapping:AbstractConceptMapping , abstractPropertyMapping:AbstractPropertyMapping 
	    , logicalTableAlias:String ) : SQLLogicalTable  = {
		
		
		val pm = abstractPropertyMapping.asInstanceOf[R2RMLPredicateObjectMap];  
		val refObjectMap = pm.getRefObjectMap();
		
		val result:SQLLogicalTable  =  {
			if(refObjectMap != null) { 
				val parentLogicalTable = refObjectMap.getParentLogicalTable();
				if(parentLogicalTable == null) {
					val errorMessage = "Parent logical table is not found for RefObjectMap : " + refObjectMap;
					logger.error(errorMessage);
				}
				val unfolder = this.owner.getUnfolder().asInstanceOf[R2RMLElementUnfoldVisitor];
				val sqlParentLogicalTableAux = unfolder.visit(parentLogicalTable);
				sqlParentLogicalTableAux;
			} else {
			  null;
			}
		}
		
		result;
	  
	}	
}