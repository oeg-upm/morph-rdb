package es.upm.fi.dia.oeg.morph.rdb.querytranslator

import scala.collection.JavaConversions._
import org.apache.jena.graph.Triple
import Zql.ZConstant
import Zql.ZSelectItem
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLSelectItem
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLPredicateObjectMap
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseBetaGenerator
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphAlphaResult
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.model.MorphBasePropertyMapping
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBUnfolder


class MorphRDBBetaGenerator(md:R2RMLMappingDocument, unfolder:MorphRDBUnfolder)
	extends MorphBaseBetaGenerator(md, unfolder) {

	override def calculateBetaObject(tp:Triple , cm:MorphBaseClassMapping , predicateURI:String
																	 , alphaResult:MorphAlphaResult , pm:MorphBasePropertyMapping ) : List[ZSelectItem] = {

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
						val databaseColumnsString = MorphRDBUnfolder.getReferencedColumns(objectMap, this.dbType)
						val betaObjectsAux = databaseColumnsString.map(databaseColumnStringAux => {
              val databaseColumnString = if(Constants.DATABASE_CSV.equalsIgnoreCase(dbType)
                && !databaseColumnStringAux.contains(" ")) {
                databaseColumnStringAux.toUpperCase()
              } else {
                databaseColumnStringAux
              }
              MorphSQLSelectItem.apply(databaseColumnString,logicalTableAlias, dbType, null)
            });

						betaObjectsAux.toList;
					}
				}
			} else {
				val parentTriplesMap = md.getParentTriplesMap(refObjectMap);
				val parentLogicalTable = parentTriplesMap.logicalTable;
				val parentSubjectMap = parentTriplesMap.subjectMap;
				val parentColumns = MorphRDBUnfolder.getReferencedColumns(parentSubjectMap, this.dbType)

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

	override def calculateBetaSubject(tp:Triple , cm:MorphBaseClassMapping , alphaResult:MorphAlphaResult )
	: List[ZSelectItem] = {

		val triplesMap = cm.asInstanceOf[R2RMLTriplesMap];
		val subjectMap = triplesMap.subjectMap;
		val logicalTableAlias = alphaResult.alphaSubject.getAlias();
		val databaseColumnsString = MorphRDBUnfolder.getReferencedColumns(subjectMap, this.dbType)

		val result:List[ZSelectItem] = {
			if(databaseColumnsString != null) {
				val resultAux = databaseColumnsString.map(databaseColumnStringAux => {
          val databaseColumnString = if(Constants.DATABASE_CSV.equalsIgnoreCase(dbType)
            && !databaseColumnStringAux.contains(" ")) {
            databaseColumnStringAux.toUpperCase()
          } else {
            databaseColumnStringAux
          }
          MorphSQLSelectItem.apply(databaseColumnString, logicalTableAlias, dbType, null)
        }

        );
				resultAux.toList;
			} else {
				Nil;
			}
		}
		result;
	}

}