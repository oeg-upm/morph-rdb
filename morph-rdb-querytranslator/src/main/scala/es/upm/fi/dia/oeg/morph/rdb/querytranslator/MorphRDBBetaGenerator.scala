package es.upm.fi.dia.oeg.morph.rdb.querytranslator

import scala.collection.JavaConversions._
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping
import es.upm.fi.dia.oeg.obdi.core.model.AbstractPropertyMapping
import com.hp.hpl.jena.graph.Triple
import Zql.ZConstant
import Zql.ZSelectItem
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLSelectItem
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLPredicateObjectMap
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.R2RMLUnfolder
import es.upm.fi.dia.oeg.obdi.core.engine.AbstractUnfolder
import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseBetaGenerator
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphAlphaResult


class MorphRDBBetaGenerator(md:R2RMLMappingDocument, unfolder:R2RMLUnfolder)
extends MorphBaseBetaGenerator(md:AbstractMappingDocument, unfolder:AbstractUnfolder) {

	override def calculateBetaObject(tp:Triple , cm:AbstractConceptMapping , predicateURI:String 
	    , alphaResult:MorphAlphaResult , pm:AbstractPropertyMapping ) : java.util.List[ZSelectItem] = {
	  
		val predicateObjectMap = pm.asInstanceOf[R2RMLPredicateObjectMap];
		val refObjectMap = predicateObjectMap.getRefObjectMap(0); 
		
		val logicalTableAlias = alphaResult.alphaSubject.getAlias();

		val betaObjects : List[MorphSQLSelectItem] = {
			if(refObjectMap == null) {
				val objectMap = predicateObjectMap.getObjectMap(0);
	
				objectMap.termMapType match {
				  case Constants.MorphTermMapType.ConstantTermMap => {
					val constantValue = objectMap.getConstantValue();
					val zConstant = new ZConstant(constantValue, ZConstant.STRING);
					val selectItem = MorphSQLSelectItem.apply(zConstant);
					List(selectItem);				    
				  }
				  case _ => {
					val databaseColumnsString = objectMap.getReferencedColumns();
					val betaObjectsAux = databaseColumnsString.map(databaseColumnString => 
					  MorphSQLSelectItem.apply(databaseColumnString,logicalTableAlias, dbType, null));
					betaObjectsAux.toList;				    
				  }
				}
			} else {
				val parentTriplesMap = md.getParentTripleMap(refObjectMap);
				val parentLogicalTable = parentTriplesMap.logicalTable;
				val parentSubjectMap = parentTriplesMap.subjectMap;
				val parentColumns = parentSubjectMap.getReferencedColumns;
				
				val refObjectMapAliasAux = this.owner.mapTripleAlias.get(tp);
				val refObjectMapAlias = if(refObjectMapAliasAux.isDefined) { refObjectMapAliasAux.get}
				else {null}
				
				if(parentColumns != null) {
					val betaObjectsAux = parentColumns.map(parentColumn => {
						MorphSQLSelectItem.apply(parentColumn, refObjectMapAlias, dbType, null);
					})
					betaObjectsAux.toList;
				} else {
				  Nil;
				}
			}
		}
		
		betaObjects;
	}

	override def calculateBetaSubject(tp:Triple , cm:AbstractConceptMapping , alphaResult:MorphAlphaResult ) 
	: java.util.List[ZSelectItem] = {
		
		val triplesMap = cm.asInstanceOf[R2RMLTriplesMap];
		val subjectMap = triplesMap.subjectMap;
		val logicalTableAlias = alphaResult.alphaSubject.getAlias();
		
		val databaseColumnsString = subjectMap.getReferencedColumns();
		
		val result:List[ZSelectItem] = {
			if(databaseColumnsString != null) {
				val resultAux = databaseColumnsString.map(databaseColumnString => 
				  MorphSQLSelectItem.apply(databaseColumnString, logicalTableAlias, dbType, null));
				resultAux.toList;
			} else {
			  Nil;
			}		  
		}
		result;
	}

}