package es.upm.fi.dia.oeg.morph.rdb.querytranslator

import scala.collection.JavaConversions._
import Zql.ZConstant
import Zql.ZExpression
import org.apache.jena.graph.Node
import org.apache.jena.graph.Triple
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLPredicateObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBUtility
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseCondSQLGenerator
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseBetaGenerator
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphAlphaResult
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLUtility
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.model.MorphBasePropertyMapping
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBUnfolder
import org.slf4j.LoggerFactory
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLRefObjectMap
import org.apache.jena.graph.impl.LiteralLabel

class MorphRDBCondSQLGenerator(md:R2RMLMappingDocument, unfolder:MorphRDBUnfolder)
  extends MorphBaseCondSQLGenerator(md, unfolder:MorphBaseUnfolder) {
  override val logger = LoggerFactory.getLogger(this.getClass());

  def genCondSQLPredicateObjectLiteral(tpObjectLiteral:LiteralLabel
                                       , objectMap:R2RMLObjectMap
                                       , refObjectMap:R2RMLRefObjectMap
                                       , logicalTableAlias:String
                                      ) : ZExpression = {
    if(refObjectMap == null && objectMap == null) {
      val errorMessage = "triple.object is a literal, but RefObjectMap is specified instead of ObjectMap";
      logger.error(errorMessage);
    }

    val objectLiteralValue = tpObjectLiteral.getValue();

    val result = if(objectMap != null) {
      val termMapType = objectMap.termMapType;
      objectMap.termMapType match {
        case Constants.MorphTermMapType.ColumnTermMap => {
          val columnName = objectMap.getColumnName();
          if(columnName != null) {
            val columnNameWithAlias = {
              if(logicalTableAlias != null && !logicalTableAlias.equals("")) {
                logicalTableAlias + "." + columnName;
              } else {
                columnName
              }
            }

            val columnConstant = new ZConstant(columnNameWithAlias,  ZConstant.COLUMNNAME);
            val objectLiteral = objectLiteralValue match {
              case literalBooleanValue:java.lang.Boolean => {
                val properties = this.owner.properties
                val objConstant = if(literalBooleanValue) {
                  val dbTrueValue = properties.databaseBooleanTrue
                  if(dbTrueValue != null) {
                    new ZConstant(dbTrueValue.toString(), ZConstant.STRING);
                  } else {
                    new ZConstant(literalBooleanValue.toString(), ZConstant.STRING);
                  }
                } else {
                  val dbFalseValue = properties.databaseBooleanFalse
                  if(dbFalseValue != null) {
                    new ZConstant(dbFalseValue.toString(), ZConstant.STRING);
                  } else {
                    new ZConstant(literalBooleanValue.toString(), ZConstant.STRING);
                  }
                }
                objConstant
              }
              case _ => {
                new ZConstant(objectLiteralValue.toString(), ZConstant.STRING);
              }
            }

            new ZExpression("=", columnConstant, objectLiteral);
          } else {
            null
          }
        }
        case Constants.MorphTermMapType.ConstantTermMap => {
          logger.info("Generating CondSQL for ConstantTermMap");
          val objectLiteral = new ZConstant(objectLiteralValue.toString(), ZConstant.STRING);
          val objectMapConstant = new ZConstant(objectMap.getConstantValue(), ZConstant.STRING);
          new ZExpression("=", objectMapConstant, objectLiteral);
        }
        case _ => {
          null
        }
      }


    } else {
      null
    }

    result;
  }

  def genCondSQLPredicateObjectURI(tpObjectURI:String
                                   , objectMap:R2RMLObjectMap
                                   , refObjectMap:R2RMLRefObjectMap
                                   , logicalTableAlias:String
                                   , refObjectMapAlias:String
                                   , cm:MorphBaseClassMapping
                                  ) : ZExpression = {
    val zConstantObjectURI = new ZConstant(tpObjectURI.toString(), ZConstant.STRING);

    val result = if(refObjectMap == null && objectMap == null) {
      null
    } else if (refObjectMap != null && objectMap != null) {
      null
    } else if(objectMap != null && refObjectMap == null) {

      val termMapType = objectMap.termMapType;
      objectMap.termMapType match {
        case Constants.MorphTermMapType.TemplateTermMap => {
          //								this.generateCondForWellDefinedURI(objectMap
          //										, uri, logicalTableAlias);
          MorphRDBUtility.generateCondForWellDefinedURI(
            objectMap, cm, tpObjectURI, logicalTableAlias)
        }
        case Constants.MorphTermMapType.ColumnTermMap => {
          val columnName = objectMap.getColumnName();
          val columnNameWithAlias = {
            if(logicalTableAlias != null) {
              logicalTableAlias + "." + columnName;
            } else {
              columnName
            }
          }

          val zConstantObjectColumn = new ZConstant(columnNameWithAlias,  ZConstant.COLUMNNAME);
          new ZExpression("=", zConstantObjectColumn, zConstantObjectURI);
        }
        case Constants.MorphTermMapType.ConstantTermMap => {
          logger.debug("Generating CondSQL for ConstantTermMap");

          val zConstantObjectConstant = new ZConstant(objectMap.getConstantValue(), ZConstant.STRING);
          new ZExpression("=", zConstantObjectConstant, zConstantObjectURI);
        }
        case _ => {
          null
        }
      }
    } else if(refObjectMap != null && objectMap == null) {
      //val refObjectMapAlias = this.owner.getTripleAlias(tp);
      val parentTriplesMap = md.getParentTriplesMap(refObjectMap);
      val parentSubjectMap = parentTriplesMap.subjectMap;
      val parentLogicalTable = parentTriplesMap.logicalTable;
      //val refObjectMapAlias = parentLogicalTable.alias;


      //Collection<R2RMLJoinCondition> joinConditions = refObjectMap.getJoinConditions();
      //ZExp onExpression = R2RMLUtility.generateJoinCondition(joinConditions, logicalTableAlias, refObjectMapAlias);
      // onExpression done in alpha generator

      //							val parentTriplesMap =
      //									refObjectMap.getParentTriplesMap().asInstanceOf[R2RMLTriplesMap];
      //val md = this.owner.getMappingDocument().asInstanceOf[R2RMLMappingDocument];

      val uriCondition = MorphRDBUtility.generateCondForWellDefinedURI(
        parentTriplesMap.subjectMap, parentTriplesMap, tpObjectURI,
        refObjectMapAlias);

      val expressionsList = List(uriCondition);
      MorphSQLUtility.combineExpresions(expressionsList, Constants.SQL_LOGICAL_OPERATOR_AND);
    } else {
      null
    }

    result;
  }

  override def genCondSQLPredicateObject(tp:Triple, alphaResult:MorphAlphaResult
                                         , betaGenerator:MorphBaseBetaGenerator
                                         , cm:MorphBaseClassMapping , pm:MorphBasePropertyMapping)
  : ZExpression = {
    logger.debug(s"Generating genCondSQLPredicateObject for tp: ${tp}");

    val tpObject = tp.getObject();
    val logicalTableAlias = alphaResult.alphaSubject.getAlias();

    val poMap = pm.asInstanceOf[R2RMLPredicateObjectMap];
    val refObjectMap = poMap.getRefObjectMap(0);
    val objectMap = poMap.getObjectMap(0);
    if(refObjectMap == null && objectMap == null) {
      val errorMessage = "no mappings is specified.";
      logger.error(errorMessage);
      null
    } else if (refObjectMap != null && objectMap != null) {
      val errorMessage = "Wrong mapping, ObjectMap and RefObjectMap shouldn't be specified at the same time.";
      logger.error(errorMessage);
    }

    val result:ZExpression = {
      if(tpObject.isLiteral()) {
        val tpObjectLiteral = tpObject.getLiteral();
        this.genCondSQLPredicateObjectLiteral(tpObjectLiteral, objectMap, refObjectMap, logicalTableAlias);
      } else if(tpObject.isURI()) {
        val tpObjectURI = tpObject.getURI();
        //val refObjectMapAlias = this.owner.mapTripleAlias(tp);
        val refObjectMapAlias = this.owner.mapTripleAlias.getOrElse(tp, null);
        this.genCondSQLPredicateObjectURI(tpObjectURI, objectMap, refObjectMap, logicalTableAlias
          , refObjectMapAlias, cm);
      } else if(tpObject.isVariable()) {
        null
      } else {
        null
      }
    }

    result;
  }

  //	def generateCondForWellDefinedURI(termMap:R2RMLTermMap, uri:String , alias:String
  //			//, columnsMetaData:Map[String, ColumnMetaData]
  //			//, tableMetaData:TableMetaData
  //			) : ZExpression = {
  //			val logicalTable = termMap.getOwner().getLogicalTable();
  //			val logicalTableMetaData = logicalTable.getTableMetaData();
  //			val conn = logicalTable.getOwner().getOwner().getConn();
  //
  //			val tableMetaData = {
  //					if(logicalTableMetaData == null && conn != null) {
  //						try {
  //							logicalTable.buildMetaData(conn);
  //							logicalTable.getTableMetaData();
  //						} catch {
  //						case e:Exception => {
  //							logger.error(e.getMessage());
  //							throw new QueryTranslationException(e.getMessage());
  //						}
  //						}
  //					} else {
  //						logicalTableMetaData
  //					}
  //			}
  //
  //			val result:ZExpression = {
  //					if(termMap.getTermMapType() == TermMapType.TEMPLATE) {
  //						val matchedColValues = termMap.getTemplateValues(uri);
  //						if(matchedColValues == null || matchedColValues.size() == 0) {
  //							val errorMessage = "uri " + uri + " doesn't match the template : " + termMap.getTemplateString();
  //							logger.debug(errorMessage);
  //							null
  //						} else {
  //							val exprs:List[ZExpression] = {
  //								val exprsAux = matchedColValues.keySet().map(pkColumnString => {
  //									val value = matchedColValues.get(pkColumnString);
  //
  //									val termMapColumnTypeName = termMap.getColumnTypeName();
  //									val columnTypeName = {
  //											if(termMapColumnTypeName != null) {
  //												termMapColumnTypeName
  //											} else {
  //												if(tableMetaData != null && tableMetaData.getColumnMetaData(pkColumnString).isDefined) {
  //													val columnTypeNameAux = tableMetaData.getColumnMetaData(pkColumnString).get.dataType;
  //													termMap.setColumnTypeName(columnTypeNameAux);
  //													columnTypeNameAux
  //												} else {
  //													null
  //												}
  //											}
  //									}
  //
  //									val pkColumnConstant = MorphSQLConstant.apply(
  //											alias + "." + pkColumnString, ZConstant.COLUMNNAME, databaseType);
  //
  //									val pkValueConstant = {
  //											if(columnTypeName != null) {
  //												if(SQLDataType.isDatatypeNumber(columnTypeName)) {
  //													new ZConstant(value, ZConstant.NUMBER);
  //												} else if(SQLDataType.isDatatypeString(columnTypeName)) {
  //													new ZConstant(value, ZConstant.STRING);
  //												} else {
  //													new ZConstant(value, ZConstant.STRING);
  //												}
  //											} else {
  //												new ZConstant(value, ZConstant.STRING);
  //											}
  //									}
  //
  //									val expr = new ZExpression("=", pkColumnConstant, pkValueConstant);
  //									expr;
  //								})
  //								exprsAux.toList;
  //						}
  //
  //						MorphSQLUtility.combineExpresions(
  //								exprs, Constants.SQL_LOGICAL_OPERATOR_AND);
  //						}
  //					} else {
  //						null
  //					}
  //			}
  //
  //			logger.debug("generateCondForWellDefinedURI = " + result);
  //			result;
  //	}

  override def genCondSQLSubjectURI(tpSubject:Node , alphaResult:MorphAlphaResult
                                    , cm:MorphBaseClassMapping ) : ZExpression = {
    val subjectURI = tpSubject.getURI();
    val tm = cm.asInstanceOf[R2RMLTriplesMap];
    val subjectURIConstant = new ZConstant(subjectURI, ZConstant.STRING);
    val logicalTableAlias = alphaResult.alphaSubject.getAlias();
    val subjectTermMapType = tm.subjectMap.termMapType;

    val result2:ZExpression = {
      subjectTermMapType match {
        case Constants.MorphTermMapType.TemplateTermMap => {
          try {
            MorphRDBUtility.generateCondForWellDefinedURI(tm.subjectMap, tm
              , tpSubject.getURI(), logicalTableAlias);
          } catch {
            case e:Exception => {
              logger.error(e.getMessage());
              throw new Exception(e);
            }
          }
        }
        case Constants.MorphTermMapType.ColumnTermMap => {
          val subjectMapColumn = new ZConstant(tm.subjectMap.getColumnName(), ZConstant.COLUMNNAME);
          new ZExpression("=", subjectMapColumn, subjectURIConstant);
        }
        case Constants.MorphTermMapType.ConstantTermMap => {
          val subjectMapColumn = new ZConstant(tm.subjectMap.getConstantValue(), ZConstant.COLUMNNAME);
          new ZExpression("=", subjectMapColumn, subjectURIConstant);
        }
        case _ => {
          val errorMessage = "Invalid term map type";
          logger.error(errorMessage);
          throw new Exception(errorMessage);
        }
      }

    }

    result2;
  }

}