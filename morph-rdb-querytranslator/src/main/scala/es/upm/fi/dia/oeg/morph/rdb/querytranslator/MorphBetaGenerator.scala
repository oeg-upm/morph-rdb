package es.upm.fi.dia.oeg.morph.rdb.querytranslator

import scala.collection.JavaConversions._

import es.upm.fi.dia.oeg.obdi.core.querytranslator.AbstractBetaGenerator
import es.upm.fi.dia.oeg.obdi.core.querytranslator.AbstractQueryTranslator
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping
import es.upm.fi.dia.oeg.obdi.core.model.AbstractPropertyMapping
import es.upm.fi.dia.oeg.obdi.core.querytranslator.AlphaResult
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLPredicateObjectMap
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTriplesMap
import com.hp.hpl.jena.graph.Triple
import Zql.ZConstant;
import Zql.ZSelectItem;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTermMap.TermMapType;
import es.upm.fi.dia.oeg.upm.morph.sql.MorphSQLSelectItem;

class MorphBetaGenerator(owner: AbstractQueryTranslator) extends AbstractBetaGenerator(owner) {

	override def calculateBetaObject(tp:Triple , cm:AbstractConceptMapping , predicateURI:String 
	    , alphaResult:AlphaResult , pm:AbstractPropertyMapping ) : java.util.List[ZSelectItem] = {
	  
		val predicateObjectMap = pm.asInstanceOf[R2RMLPredicateObjectMap];
		val refObjectMap = predicateObjectMap.getRefObjectMap(); 
		
		val logicalTableAlias = alphaResult.getAlphaSubject().getAlias();
		val dbType = this.owner.getDatabaseType();

		val betaObjects : List[MorphSQLSelectItem] = {
			if(refObjectMap == null) {
				val objectMap = predicateObjectMap.getObjectMap();
	
				if(objectMap.getTermMapType() == TermMapType.CONSTANT) {
					val constantValue = objectMap.getConstantValue();
					val zConstant = new ZConstant(constantValue, ZConstant.STRING);
					val selectItem = MorphSQLSelectItem.apply(zConstant);
					List(selectItem);
				} else {
					val databaseColumnsString = objectMap.getDatabaseColumnsString();
					val betaObjectsAux = databaseColumnsString.map(databaseColumnString => 
					  MorphSQLSelectItem.apply(databaseColumnString,logicalTableAlias, dbType, null));
					betaObjectsAux.toList;
					
//					val betaObjectsAux = for(databaseColumnString <- databaseColumnsString) yield {
//						MorphSQLSelectItem.apply(databaseColumnString,logicalTableAlias, dbType, null)
//					}					  
//					betaObjectsAux.toList;

								
	//				for(databaseColumnString <- databaseColumnsString) {
	//					val selectItem = MorphSQLSelectItem.apply(
	//							databaseColumnString,logicalTableAlias, dbType, null);
	//					betaObjects = betaObjects ::: List(selectItem);
	//				}
				}
			} else {
				val databaseColumnsString = refObjectMap.getParentDatabaseColumnsString();
				val refObjectMapAlias = this.owner.getTripleAlias(tp);
				
				if(databaseColumnsString != null) {
					val betaObjectsAux = databaseColumnsString.map(databaseColumnString => {
						MorphSQLSelectItem.apply(databaseColumnString, refObjectMapAlias, dbType, null);
					})
					betaObjectsAux.toList;
					
//					val betaObjectsAux2 = for(databaseColumnString <- databaseColumnsString) yield {
//					  MorphSQLSelectItem.apply(databaseColumnString, refObjectMapAlias, dbType, null);
//					}
//					betaObjectsAux2.toList;
					
	//				for(databaseColumnString <- databaseColumnsString) {
	//					val selectItem = MorphSQLSelectItem.apply(
	//							databaseColumnString, refObjectMapAlias, dbType, null);
	//					
	//					betaObjects = betaObjects ::: List(selectItem);
	//				}
				} else {
				  Nil;
				}
			}
		  
		}
		
		betaObjects;
	}

	override def calculateBetaSubject(tp:Triple , cm:AbstractConceptMapping , alphaResult:AlphaResult ) 
	: java.util.List[ZSelectItem] = {
		
		val triplesMap = cm.asInstanceOf[R2RMLTriplesMap];
		val subjectMap = triplesMap.getSubjectMap();
		val dbType = this.owner.getDatabaseType();
		val logicalTableAlias = alphaResult.getAlphaSubject().getAlias();
		
		val databaseColumnsString = 
				subjectMap.getDatabaseColumnsString();
		
		val result:List[ZSelectItem] = {
			if(databaseColumnsString != null) {
				val resultAux = databaseColumnsString.map(databaseColumnString => 
				  MorphSQLSelectItem.apply(databaseColumnString, logicalTableAlias, dbType, null));
				resultAux.toList;
	//			for(databaseColumnString <- databaseColumnsString) yield {
	//				MorphSQLSelectItem.apply(databaseColumnString, logicalTableAlias, dbType, null);
	//			}
			} else {
			  Nil;
			}		  
		}
		result;
	}

}