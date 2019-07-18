package es.upm.fi.dia.oeg.morph.rdb.querytranslator

import scala.collection.JavaConversions._
import org.apache.jena.graph.Node
import org.apache.jena.graph.Triple
import org.apache.jena.vocabulary.RDF
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBUnfolder
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLPredicateObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.base.querytranslator.{MorphAlphaResultPredicateObject, MorphBaseAlphaGenerator, MorphAlphaResult, MorphBaseQueryTranslator}
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLLogicalTable
import es.upm.fi.dia.oeg.morph.base.model.MorphBasePropertyMapping
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.sql.SQLJoinTable
import es.upm.fi.dia.oeg.morph.base.sql.SQLLogicalTable
import es.upm.fi.dia.oeg.morph.base.sql.SQLFromItem
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import org.slf4j.LoggerFactory

class MorphRDBAlphaGenerator(md:R2RMLMappingDocument,unfolder:MorphRDBUnfolder)
  extends MorphBaseAlphaGenerator(md,unfolder)
{
  val databaseType = if(md.dbMetaData.isDefined) {
    md.dbMetaData.get.dbType;
  } else {
    Constants.DATABASE_DEFAULT
  }
  val enclosedCharacter = Constants.getEnclosedCharacter(this.databaseType);

  override val logger = LoggerFactory.getLogger(this.getClass());


  override def calculateAlpha(tp:Triple, abstractConceptMapping:MorphBaseClassMapping
                              , predicateURI:String ) : MorphAlphaResult = {
    //ALPHA SUBJECT
    val tpSubject = tp.getSubject();
    val alphaSubject = this.calculateAlphaSubject(tpSubject, abstractConceptMapping);
    val logicalTableAlias = alphaSubject.getAlias();

    val pmsAux = abstractConceptMapping.getPropertyMappings(predicateURI);
    val alphaResult : MorphAlphaResult = {
      if(RDF.`type`.getURI().equalsIgnoreCase(predicateURI)) {
        val alphaResultPredicateObject = new MorphAlphaResultPredicateObject(null, predicateURI);
        val alphaPO = List(alphaResultPredicateObject);
        new MorphAlphaResult(alphaSubject, alphaPO);
      } else {
        val pms = { if (pmsAux == null) {Nil}
        else {pmsAux}
        }

        //ALPHA PREDICATE OBJECT
        val alphaPredicateObjects:List[MorphAlphaResultPredicateObject] = {
          if(pms.size > 1) {
            val errorMessage = "Multiple mappings of a predicate is not supported.";
            logger.error(errorMessage);
          }

          val pm = pms.iterator.next().asInstanceOf[R2RMLPredicateObjectMap];
          val refObjectMap = pm.getRefObjectMap(0);
          if(refObjectMap != null) {
            val alphaPredicateObject = this.calculateAlphaPredicateObject(
              tp, abstractConceptMapping, pm, alphaSubject);
            if(alphaPredicateObject.isEmpty) { Nil }
            else {
              val parentLogicalTable = alphaPredicateObject.get.parentTable.joinSource;
              val parentAndChildHaveSameLogicalTable = alphaSubject.sameTableWith(parentLogicalTable);

              List(alphaPredicateObject.get);
            }

          } else {
            Nil;
          }

        }

        new MorphAlphaResult(alphaSubject, alphaPredicateObjects);

      }
    }

    alphaResult;
  }

  override def calculateAlpha(tp:Triple, abstractConceptMapping:MorphBaseClassMapping
                              , predicateURI:String , pm:MorphBasePropertyMapping ) : MorphAlphaResult = {
    null;
  }

  override def calculateAlphaPredicateObject(triple:Triple
                                             , abstractConceptMapping:MorphBaseClassMapping
                                             , abstractPropertyMapping:MorphBasePropertyMapping
                                             , alphaSubject:SQLLogicalTable )
  : Option[MorphAlphaResultPredicateObject] = {
    val alphaSubjectAlias = alphaSubject.getAlias();

    val tpPredicate = triple.getPredicate;
    val tpObject = triple.getObject;
    val isRDFTypeStatement = tpPredicate.isURI && RDF.`type`.getURI().equals(tpPredicate.getURI) && tpObject.isURI;

    val alphaPredicateObject:Option[MorphAlphaResultPredicateObject] = if(isRDFTypeStatement) { None}
    else {
      val pm = abstractPropertyMapping.asInstanceOf[R2RMLPredicateObjectMap];
      val refObjectMap = pm.getRefObjectMap(0);

      val result:SQLJoinTable  =  {
        if(refObjectMap != null) {
          //				val parentLogicalTable = refObjectMap.getParentLogicalTable().asInstanceOf[R2RMLLogicalTable];
          //val md = this.owner.getMappingDocument().asInstanceOf[R2RMLMappingDocument];
          val parentTriplesMap = md.getParentTriplesMap(refObjectMap);
          val parentLogicalTable = parentTriplesMap.getLogicalTable.asInstanceOf[R2RMLLogicalTable];

          if(parentLogicalTable == null) {
            val errorMessage = "Parent logical table is not found for RefObjectMap : " + refObjectMap;
            logger.error(errorMessage);
            null;
          } else {
            //val unfolder = this.owner.getUnfolder().asInstanceOf[R2RMLUnfolder];
            val sqlParentLogicalTableAux = unfolder.visit(parentLogicalTable);
            val sqlParentLogicalTable = new SQLJoinTable(sqlParentLogicalTableAux
              , Constants.JOINS_TYPE_INNER, null);
            val childTableIsSameWithParentTable = alphaSubject.sameTableWith(sqlParentLogicalTableAux);

            val sqlParentLogicalTableAuxAlias = sqlParentLogicalTableAux.generateAlias();
            this.owner.mapTripleAlias += (triple -> sqlParentLogicalTableAuxAlias);
            val joinQueryAlias = sqlParentLogicalTableAuxAlias;

            val joinConditions = refObjectMap.getJoinConditions();
            if(childTableIsSameWithParentTable && (joinConditions == null || joinConditions.isEmpty)) {
              null;
            } else {
              val onExpression = MorphRDBUnfolder.unfoldJoinConditions(
                joinConditions, alphaSubjectAlias, joinQueryAlias
                , databaseType);
              if(onExpression != null) {
                sqlParentLogicalTable.onExpression = onExpression;
              }

              sqlParentLogicalTable;
            }
          }
        } else {
          null
        }
      }

      val predicateURI = pm.getMappedPredicateName(0)
      val alphaResultPredicateObject = new MorphAlphaResultPredicateObject(result, predicateURI);
      Some(alphaResultPredicateObject);
    }
    alphaPredicateObject;
  }

  override def calculateAlphaSubject(subject:Node, abstractConceptMapping:MorphBaseClassMapping )
  : SQLLogicalTable = {
    val cm = abstractConceptMapping.asInstanceOf[R2RMLTriplesMap];
    val r2rmlLogicalTable = cm.getLogicalTable().asInstanceOf[R2RMLLogicalTable];
    //val unfolder = this.owner.getUnfolder().asInstanceOf[R2RMLUnfolder];
    val sqlLogicalTable = r2rmlLogicalTable.accept(unfolder).asInstanceOf[SQLLogicalTable]



    val cmLogicalTableAlias = r2rmlLogicalTable.alias;
    val logicalTableAlias = {
      if(cmLogicalTableAlias == null || cmLogicalTableAlias.equals("")) {
        sqlLogicalTable.generateAlias();
        //enclosedCharacter + sqlLogicalTable.generateAlias() + enclosedCharacter
      } else {
        cmLogicalTableAlias
      }
    }

    sqlLogicalTable.setAlias(logicalTableAlias);
    sqlLogicalTable.databaseType = this.databaseType;
    return sqlLogicalTable;
  }



  //	override def calculateAlphaPredicateObjectSTG2(tp:Triple , cm:MorphBaseClassMapping
  //	    , tpPredicateURI:String , logicalTableAlias:String ) : List[SQLLogicalTable] = {
  //
  //		val isRDFTypeStatement = RDF.`type`.getURI().equals(tpPredicateURI);
  //
  //		val alphaPredicateObjects : List[SQLLogicalTable] = {
  //			if(isRDFTypeStatement) {
  //				//do nothing
  //			  Nil;
  //			} else {
  //				val pms = cm.getPropertyMappings(tpPredicateURI);
  //				if(pms != null && !pms.isEmpty()) {
  //					val pm = pms.iterator.next().asInstanceOf[R2RMLPredicateObjectMap];
  //					val refObjectMap = pm.getRefObjectMap(0);
  //					if(refObjectMap != null) {
  //						val alphaPredicateObject =
  //								this.calculateAlphaPredicateObject2(tp, cm, pm, logicalTableAlias);
  //						List(alphaPredicateObject);
  //					} else {
  //					  Nil;
  //					}
  //				} else {
  //					if(!isRDFTypeStatement) {
  //						val errorMessage = "Undefined mapping for : " + tpPredicateURI + " in : " + cm.toString();
  //						logger.error(errorMessage);
  //						Nil;
  //					} else {
  //					  Nil;
  //					}
  //				}
  //			}
  //		}
  //
  //		alphaPredicateObjects;
  //	}
  //
  //	override def calculateAlphaPredicateObject2(triple:Triple
  //	    , abstractConceptMapping:MorphBaseClassMapping , abstractPropertyMapping:MorphBasePropertyMapping
  //	    , logicalTableAlias:String ) : SQLLogicalTable  = {
  //
  //
  //		val pm = abstractPropertyMapping.asInstanceOf[R2RMLPredicateObjectMap];
  //		val refObjectMap = pm.getRefObjectMap(0);
  //
  //		val result:SQLLogicalTable  =  {
  //			if(refObjectMap != null) {
  //				//val parentLogicalTable = refObjectMap.getParentLogicalTable().asInstanceOf[R2RMLLogicalTable];
  //				//val md = this.owner.getMappingDocument().asInstanceOf[R2RMLMappingDocument];
  //			  val parentTriplesMap = md.getParentTriplesMap(refObjectMap);
  //				val parentLogicalTable = parentTriplesMap.logicalTable.asInstanceOf[R2RMLLogicalTable];
  //				if(parentLogicalTable == null) {
  //					val errorMessage = "Parent logical table is not found for RefObjectMap : " + refObjectMap;
  //					logger.error(errorMessage);
  //				}
  //				//val unfolder = this.owner.getUnfolder().asInstanceOf[R2RMLUnfolder];
  //				val sqlParentLogicalTableAux = unfolder.visit(parentLogicalTable);
  //				sqlParentLogicalTableAux;
  //			} else {
  //			  null;
  //			}
  //		}
  //
  //		result;
  //
  //	}
}