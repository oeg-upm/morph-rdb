package es.upm.fi.dia.oeg.morph.rdb.querytranslator

import scala.collection.JavaConversions._
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping
import es.upm.fi.dia.oeg.obdi.core.model.AbstractPropertyMapping
import es.upm.fi.dia.oeg.obdi.core.sql.SQLLogicalTable
import es.upm.fi.dia.oeg.obdi.core.sql.SQLJoinTable
import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.vocabulary.RDF
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.R2RMLUnfolder
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLPredicateObjectMap
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.obdi.core.engine.AbstractUnfolder
import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseAlphaGenerator
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphAlphaResult
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLLogicalTable
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator

class MorphRDBAlphaGenerator(md:R2RMLMappingDocument,unfolder:R2RMLUnfolder)
//(owner:IQueryTranslator) 
extends 
MorphBaseAlphaGenerator(md:AbstractMappingDocument,unfolder:AbstractUnfolder)
//(owner:IQueryTranslator) 
{
	override val logger = Logger.getLogger("MorphQueryTranslator");
	
	override def calculateAlpha(tp:Triple, abstractConceptMapping:AbstractConceptMapping 
	    , predicateURI:String ) : MorphAlphaResult = {
		//ALPHA SUBJECT
		val tpSubject = tp.getSubject();
		val alphaSubject = this.calculateAlphaSubject(tpSubject, abstractConceptMapping);
		val logicalTableAlias = alphaSubject.getAlias();

		val pmsAux = abstractConceptMapping.getPropertyMappings(predicateURI);
		val alphaResult : MorphAlphaResult = {
			if(RDF.`type`.getURI().equalsIgnoreCase(predicateURI)) {
				new MorphAlphaResult(alphaSubject, null, predicateURI);
			} else {
			  val pms : Iterable[AbstractPropertyMapping]= {
			    if (pmsAux == null) {Nil}
			    else {pmsAux}
			  }
					
			//ALPHA PREDICATE OBJECT
			val alphaPredicateObjects:List[SQLJoinTable] = {
				if(pms.size() > 1) {
					val errorMessage = "Multiple mappings of a predicate is not supported.";
					logger.error(errorMessage);
				}
							
				val pm = pms.iterator.next().asInstanceOf[R2RMLPredicateObjectMap];
				val refObjectMap = pm.getRefObjectMap(0);
				if(refObjectMap != null) { 
					val alphaPredicateObject = this.calculateAlphaPredicateObject(
							tp, abstractConceptMapping, pm, logicalTableAlias);
					List(alphaPredicateObject);
				} else {
					Nil;
				}

			  }
					
			  new MorphAlphaResult(alphaSubject, alphaPredicateObjects, predicateURI);

			}		  
		}

		alphaResult;
	} 

	override def calculateAlpha(tp:Triple, abstractConceptMapping:AbstractConceptMapping 
	    , predicateURI:String , pm:AbstractPropertyMapping ) : MorphAlphaResult = {
	  null;
	} 

	override def calculateAlphaPredicateObject(triple:Triple
	    , abstractConceptMapping:AbstractConceptMapping 
	    , abstractPropertyMapping:AbstractPropertyMapping, logicalTableAlias:String ) 
	: SQLJoinTable = {
		
		val pm = abstractPropertyMapping.asInstanceOf[R2RMLPredicateObjectMap];  
		val refObjectMap = pm.getRefObjectMap(0);
		
		val result:SQLJoinTable  =  {
			if(refObjectMap != null) { 
//				val parentLogicalTable = refObjectMap.getParentLogicalTable().asInstanceOf[R2RMLLogicalTable];
				//val md = this.owner.getMappingDocument().asInstanceOf[R2RMLMappingDocument];
				val parentTriplesMap = md.getParentTripleMap(refObjectMap);
				val parentLogicalTable = parentTriplesMap.getLogicalTable.asInstanceOf[R2RMLLogicalTable];
				
				if(parentLogicalTable == null) {
					val errorMessage = "Parent logical table is not found for RefObjectMap : " + refObjectMap;
					logger.error(errorMessage);
				}
				
				//val unfolder = this.owner.getUnfolder().asInstanceOf[R2RMLUnfolder];
				val sqlParentLogicalTableAux = unfolder.visit(parentLogicalTable);
				val sqlParentLogicalTable = new SQLJoinTable(sqlParentLogicalTableAux
				    , Constants.JOINS_TYPE_INNER, null);
				
				val sqlParentLogicalTableAuxAlias = sqlParentLogicalTableAux.generateAlias(); 
				this.owner.mapTripleAlias += (triple -> sqlParentLogicalTableAuxAlias);
				val joinQueryAlias = sqlParentLogicalTableAuxAlias;
	
				val joinConditions = refObjectMap.getJoinConditions();
				val onExpression = R2RMLUnfolder.unfoldJoinConditions(
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
		val r2rmlLogicalTable = cm.getLogicalTable().asInstanceOf[R2RMLLogicalTable];
		//val unfolder = this.owner.getUnfolder().asInstanceOf[R2RMLUnfolder];
		val sqlLogicalTable = unfolder.visit(r2rmlLogicalTable);

		
		val cmLogicalTableAlias = r2rmlLogicalTable.alias;
		val logicalTableAlias = {
			if(cmLogicalTableAlias == null || cmLogicalTableAlias.equals("")) {
				sqlLogicalTable.generateAlias();
			} else {
			  cmLogicalTableAlias
			}		  
		}

		sqlLogicalTable.setAlias(logicalTableAlias);
		sqlLogicalTable.setDbType(this.databaseType);
		return sqlLogicalTable;
	}
	
	override def calculateAlphaPredicateObjectSTG(tp:Triple ,cm:AbstractConceptMapping 
	    , tpPredicateURI:String , logicalTableAlias:String ) : List[SQLJoinTable] = {
		
		
		val isRDFTypeStatement = RDF.`type`.getURI().equals(tpPredicateURI);
		val  alphaPredicateObjects:List[SQLJoinTable] = {
			if(isRDFTypeStatement) {
				//do nothing
			  Nil;
			} else {
				val pms = cm.getPropertyMappings(tpPredicateURI);
				if(pms != null && !pms.isEmpty()) {
					val pm = pms.iterator().next().asInstanceOf[R2RMLPredicateObjectMap];
					val refObjectMap = pm.getRefObjectMap(0);
					if(refObjectMap != null) { 
						val alphaPredicateObject = this.calculateAlphaPredicateObject(tp, cm, pm, logicalTableAlias);
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
	    , tpPredicateURI:String , logicalTableAlias:String ) : List[SQLLogicalTable] = {
		
		val isRDFTypeStatement = RDF.`type`.getURI().equals(tpPredicateURI);
		
		val alphaPredicateObjects : List[SQLLogicalTable] = {
			if(isRDFTypeStatement) {
				//do nothing
			  Nil;
			} else {
				val pms = cm.getPropertyMappings(tpPredicateURI);
				if(pms != null && !pms.isEmpty()) {
					val pm = pms.iterator().next().asInstanceOf[R2RMLPredicateObjectMap];
					val refObjectMap = pm.getRefObjectMap(0);
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
		val refObjectMap = pm.getRefObjectMap(0);
		
		val result:SQLLogicalTable  =  {
			if(refObjectMap != null) {
				//val parentLogicalTable = refObjectMap.getParentLogicalTable().asInstanceOf[R2RMLLogicalTable];
				//val md = this.owner.getMappingDocument().asInstanceOf[R2RMLMappingDocument];
				val parentTriplesMap = md.getParentTripleMap(refObjectMap);
				val parentLogicalTable = parentTriplesMap.logicalTable.asInstanceOf[R2RMLLogicalTable];
				if(parentLogicalTable == null) {
					val errorMessage = "Parent logical table is not found for RefObjectMap : " + refObjectMap;
					logger.error(errorMessage);
				}
				//val unfolder = this.owner.getUnfolder().asInstanceOf[R2RMLUnfolder];
				val sqlParentLogicalTableAux = unfolder.visit(parentLogicalTable);
				sqlParentLogicalTableAux;
			} else {
			  null;
			}
		}
		
		result;
	  
	}	
}