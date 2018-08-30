package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import scala.collection.JavaConversions._
import java.util.Collection
//import java.util.Properties
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLSelectItem
import es.upm.fi.dia.oeg.morph.base.Constants
import java.util.HashSet
import Zql.ZQuery
import Zql.ZSelectItem
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLPredicateObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLRefObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLSubjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLPredicateMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLLogicalTable
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTable
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLSQLQuery
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLJoinCondition
import Zql.ZExpression
import Zql.ZConstant
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLUtility
import es.upm.fi.dia.oeg.morph.r2rml.MorphR2RMLElementVisitor
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.sql.SQLLogicalTable
import es.upm.fi.dia.oeg.morph.base.sql.SQLFromItem
import es.upm.fi.dia.oeg.morph.base.sql.SQLQuery
import es.upm.fi.dia.oeg.morph.base.sql.SQLJoinTable
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.base.sql.IQuery
import org.slf4j.LoggerFactory
import es.upm.fi.dia.oeg.morph.base.RegexUtility

//import es.upm.fi.dia.oeg.morph.base.MorphProperties

class MorphRDBUnfolder(md:R2RMLMappingDocument, properties:MorphRDBProperties)
  extends MorphBaseUnfolder(md, properties) with MorphR2RMLElementVisitor {
  val logger = LoggerFactory.getLogger(this.getClass());

  var mapTermMapColumnsAliases:Map[Object, List[String]] = Map.empty;

  var mapRefObjectMapAlias:Map[R2RMLRefObjectMap, String] = Map.empty;
  val dbType = properties.databaseType;
  val dbEnclosedCharacter = Constants.getEnclosedCharacter(dbType);

  def getAliases(termMapOrRefObjectMap:Object ) : Collection[String] = {
    if(this.mapTermMapColumnsAliases.get(termMapOrRefObjectMap).isDefined) {
      this.mapTermMapColumnsAliases(termMapOrRefObjectMap);
    } else {
      null
    }
  }

  def getMapRefObjectMapAlias() : Map[R2RMLRefObjectMap, String] = {
    return mapRefObjectMapAlias;
  }

  def unfoldLogicalTable(logicalTable:R2RMLLogicalTable) : SQLLogicalTable = {
    //val dbType = md.configurationProperties.databaseType;
    //		val dbType = if(md.dbMetaData.isDefined) { md.dbMetaData.get.dbType; }
    //		else { Constants.DATABASE_DEFAULT }


    val logicalTableType = logicalTable.logicalTableType;
    val result = logicalTableType match {
      case Constants.LogicalTableType.TABLE_NAME => {
        val logicalTableValue = logicalTable.getValue();


        val logicalTableValueWithEnclosedChar = logicalTableValue.replaceAllLiterally("\\\"", dbEnclosedCharacter);


        val resultAux = new SQLFromItem(logicalTableValueWithEnclosedChar
          , Constants.LogicalTableType.TABLE_NAME);
        resultAux.databaseType = this.dbType;
        resultAux
      }
      case Constants.LogicalTableType.QUERY_STRING => {
        val logicalTableValue = logicalTable.getValue();

        //this line is needed when using " in sql query.
        val sqlString = logicalTableValue.replaceAllLiterally("\\\"", dbEnclosedCharacter);

        //val sqlString = logicalTableValue;
        try {
          val sqlString2 = if(!sqlString.endsWith(";")) {
            sqlString + ";";
          } else {
            sqlString
          }
          MorphRDBUtility.toSQLQuery(sqlString2);
        }
        catch {
          case e:Exception => {
            logger.debug("Not able to parse the query, string will be used.");
            val resultAux = new SQLFromItem(sqlString, Constants.LogicalTableType.QUERY_STRING);
            resultAux.databaseType = this.dbType;
            resultAux
          }
        }
      }
      case _ => {
        logger.warn("Invalid logical table type");
        null;
      }
    }


    result;
  }

  def unfoldTermMap(termMap:R2RMLTermMap, logicalTableAlias:String)
  : List[MorphSQLSelectItem] =  {

    val result = if(termMap != null) {
      termMap.termMapType match {
        case Constants.MorphTermMapType.TemplateTermMap => {
          val termMapTemplateString = termMap.templateString.replaceAllLiterally("\\\"", dbEnclosedCharacter);
          val termMapReferencedColumns = RegexUtility.getTemplateColumns(termMapTemplateString, true).toList;

          //val termMapReferencedColumns = termMap.getReferencedColumns();
          if(termMapReferencedColumns != null) {
            termMapReferencedColumns.map(termMapReferencedColumn => {
              val selectItem = MorphSQLSelectItem.apply(
                termMapReferencedColumn, logicalTableAlias, dbType);
              if(selectItem != null) {
                if(selectItem.getAlias() == null) {
                  val selectItemColumnAux = selectItem.printColumnWithoutEnclosedChar();
                  //http://www.h2database.com/html/functions.html#csvread
                  //column names that contain no special characters (only letters, '_', and digits; similar to the rule for Java identifiers) are considered case insensitive.
                  // Other column names are case sensitive, that means you need to use quoted identifiers
                  val selectItemColumn = if(Constants.DATABASE_CSV.equalsIgnoreCase(dbType)
                    && !selectItemColumnAux.contains(" ")) {
                    selectItemColumnAux.toUpperCase()
                  } else {
                    selectItemColumnAux
                  }
                  val alias = selectItem.getTable() + "_" + selectItemColumn;
                  selectItem.setAlias(alias);
                  if(this.mapTermMapColumnsAliases.containsKey(termMap)) {
                    val oldColumnAliases = this.mapTermMapColumnsAliases(termMap);
                    val newColumnAliases = oldColumnAliases ::: List(alias);
                    this.mapTermMapColumnsAliases += (termMap -> newColumnAliases);
                  } else {
                    this.mapTermMapColumnsAliases += (termMap -> List(alias));
                  }
                }
              }
              selectItem
            });
          } else { Nil }
        }
        case Constants.MorphTermMapType.ColumnTermMap => {
          val termColumnNameWithoutEnclosedChar = termMap.columnName;
          val termColumnNameWithEnclosedCharAux = termColumnNameWithoutEnclosedChar.replaceAllLiterally("\\\"", dbEnclosedCharacter);
          val termColumnNameWithEnclosedChar = if(Constants.DATABASE_CSV.equalsIgnoreCase(dbType)
            && !termColumnNameWithEnclosedCharAux.contains(" ")) {
            termColumnNameWithEnclosedCharAux.toUpperCase()
          } else {
            termColumnNameWithEnclosedCharAux
          }

          val selectItem = MorphSQLSelectItem.apply(termColumnNameWithEnclosedChar, logicalTableAlias, dbType);

          if(selectItem != null) {
            if(selectItem.getAlias() == null) {
              val alias = selectItem.getTable() + "_" + selectItem.printColumnWithoutEnclosedChar();
              selectItem.setAlias(alias);
              if(this.mapTermMapColumnsAliases.containsKey(termMap)) {
                val oldColumnAliases = this.mapTermMapColumnsAliases(termMap);
                val newColumnAliases = oldColumnAliases ::: List(alias);
                this.mapTermMapColumnsAliases += (termMap -> newColumnAliases);
              } else {
                this.mapTermMapColumnsAliases += (termMap -> List(alias));
              }
            }
          }
          List(selectItem)
        }
        case Constants.MorphTermMapType.ConstantTermMap => {
          Nil;
        }
        case _ => {
          throw new Exception("Invalid term map type!");
        }
      }

    } else {
      Nil
    }

    result
  }


  /**
    * Contributor: Franck Michel
    * Unfolding a triples map means to progressively build an SQL query by accumulating pieces:
    * (1) create the FROM clause from the logical table,
    * (2) for each column in the subject predicate and object maps, add items to the SELECT clause,
    * (3) for each column in the parent triples map of each referencing object map, add items of the SELECT clause,
    * (4) for each join condition, add an SQL WHERE condition and an alias in the FROM clause for the parent table,
    *
    * @return an SQLQuery (IQuery) describing the actual SQL query to be run against the RDB
    */
  def  unfoldTriplesMap(triplesMapId:String, logicalTable:R2RMLLogicalTable
                        , subjectMap:R2RMLSubjectMap, poms:Collection[R2RMLPredicateObjectMap] ) : IQuery = {
    //		val triplesMap = subjectMap.getOwner();
    //		logger.info("unfolding triplesMap : " + triplesMap);

    //unfold subjectMap
    //R2RMLLogicalTable logicalTable = triplesMap.getLogicalTable();
    //val result = this.unfoldSubjectMap(subjectMap, logicalTable);

    val result = new SQLQuery();
    result.setDatabaseType(this.dbType);

    result.setDistinct(properties.materializationDistinct)

    //UNFOLD LOGICAL TABLE
    val logicalTableUnfolded :SQLFromItem = logicalTable match {
      case _:R2RMLTable => {
        this.unfoldLogicalTable(logicalTable).asInstanceOf[SQLFromItem];
      }
      case _:R2RMLSQLQuery => {
        val logicalTableAux = this.unfoldLogicalTable(logicalTable)
        logicalTableAux match {
          case _:SQLQuery => {
            val zQuery = this.unfoldLogicalTable(logicalTable).asInstanceOf[ZQuery];
            val resultAux = new SQLFromItem(zQuery.toString(), Constants.LogicalTableType.QUERY_STRING);
            resultAux.databaseType = this.dbType
            resultAux
          }
          case sqlFromItem:SQLFromItem => { sqlFromItem; }
          case _ => { null}
        }
      }
      case _ => {null}
    }
    val logicalTableAlias = logicalTableUnfolded.generateAlias();
    logicalTable.alias = logicalTableAlias;
    //result.addFrom(logicalTableUnfolded);
    val logicalTableUnfoldedJoinTable = new SQLJoinTable(logicalTableUnfolded, null, null);
    result.addFromItem(logicalTableUnfoldedJoinTable);

    val subjectMapSelectItems = this.unfoldTermMap(subjectMap, logicalTableAlias);
    result.addSelectItems(subjectMapSelectItems);


    //val logicalTableAlias = logicalTable.getAlias();
    if(poms != null) {
      for(pom <- poms) {
        //UNFOLD PREDICATEMAP
        val predicateMaps = pom.predicateMaps;
        if(predicateMaps != null && !predicateMaps.isEmpty()) {
          //val predicateMap = pom.getPredicateMap(0);
          for (pm <- pom.predicateMaps) {
            val predicateMapSelectItems = this.unfoldTermMap(pm, logicalTableAlias);
            result.addSelectItems(predicateMapSelectItems);
          }
        }


        //UNFOLD OBJECTMAP
        val objectMaps = pom.objectMaps;
        if(objectMaps != null && !objectMaps.isEmpty) {
          //val objectMap = pom.getObjectMap(0);
          for (om <- pom.objectMaps) {
            val objectMapSelectItems = this.unfoldTermMap(om, logicalTableAlias);
            //logger.info(s"objectMapSelectItems = ${objectMapSelectItems}")
            result.addSelectItems(objectMapSelectItems);
          }
        }


        //UNFOLD REFOBJECTMAP
        val refObjectMaps = pom.refObjectMaps;
        if(refObjectMaps != null && !refObjectMaps.isEmpty) {
          //val refObjectMap = pom.getRefObjectMap(0);
          refObjectMaps.filter(x => x!= null).map( refObjectMap => {
            val joinConditions = refObjectMap.getJoinConditions();

            val parentTriplesMap = this.md.getParentTriplesMap(refObjectMap);
            val parentLogicalTable = parentTriplesMap.getLogicalTable();
            if(parentLogicalTable == null) {
              val errorMessage = "Parent logical table is not found for RefObjectMap : " + pom.getMappedPredicateName(0);
              throw new Exception(errorMessage);
            }
            val sqlParentLogicalTable = this.unfoldLogicalTable(parentLogicalTable.asInstanceOf[R2RMLLogicalTable]);
            //parentLogicalTable.alias = parentLogicalTableAlias;

            val parentAndChildHaveSameLogicalTable = logicalTableUnfolded.sameTableWith(sqlParentLogicalTable);
            val noJoinConditionSpecified = (joinConditions == null || joinConditions.isEmpty);


            val parentLogicalTableAlias = if(parentAndChildHaveSameLogicalTable && noJoinConditionSpecified) { logicalTableAlias }
            else {
              sqlParentLogicalTable.generateAlias();
            }

            sqlParentLogicalTable.setAlias(parentLogicalTableAlias);
            //refObjectMap.setAlias(joinQueryAlias);
            this.mapRefObjectMapAlias += (refObjectMap -> parentLogicalTableAlias);
            pom.setAlias(parentLogicalTableAlias);




            //val refObjectMapColumnsString = refObjectMap.getParentDatabaseColumnsString();
            val parentSubjectMap = parentTriplesMap.subjectMap;
            //val refObjectMapColumnsString = parentSubjectMap.getReferencedColumns;
            val refObjectMapColumnsString = MorphRDBUnfolder.getReferencedColumns(parentSubjectMap, this.dbType)

            if(refObjectMapColumnsString != null ) {
              for(refObjectMapColumnString <- refObjectMapColumnsString) {
                val selectItem = MorphSQLSelectItem(
                  refObjectMapColumnString, parentLogicalTableAlias, dbType, null);
                if(selectItem.getAlias() == null) {
                  //val alias = selectItem.getTable() + "_" + selectItem.getColumn();
                  val alias = selectItem.getTable() + "_" + selectItem.printColumnWithoutEnclosedChar();
                  selectItem.setAlias(alias);
                  if(this.mapTermMapColumnsAliases.containsKey(refObjectMap)) {
                    val oldColumnAliases = this.mapTermMapColumnsAliases(refObjectMap);
                    val newColumnAliases = oldColumnAliases ::: List(alias);
                    this.mapTermMapColumnsAliases += (refObjectMap -> newColumnAliases);
                  } else {
                    this.mapTermMapColumnsAliases +=(refObjectMap -> List(alias));
                  }
                }
                //resultSelectItems.add(selectItem);
                result.addSelectItem(selectItem);
              }
            }



            if(parentAndChildHaveSameLogicalTable && noJoinConditionSpecified) {
              //no need to join tables
            } else {
              val onExpression = MorphRDBUnfolder.unfoldJoinConditions(
                joinConditions, logicalTableAlias, parentLogicalTableAlias, dbType);
              val joinQuery = new SQLJoinTable(sqlParentLogicalTable
                , Constants.JOINS_TYPE_LEFT, onExpression);
              //result.addJoinQuery(joinQuery);
              result.addFromItem(joinQuery);
            }

          })


        }


      }
    }

    //		if(resultSelectItems != null) {
    //			for(ZSelectItem selectItem : resultSelectItems) {
    //				result.addSelect(selectItem);
    //			}
    //		}
    //logger.info(triplesMap + " unfolded = \n" + result);

    try {
      val sliceString = this.properties.mapDataTranslationLimits.find(_._1.equals(triplesMapId));
      if(sliceString.isDefined) {
        val sliceLong = sliceString.get._2.toLong;
        result.setSlice(sliceLong);
      }

      val offsetString = this.properties.mapDataTranslationOffsets.find(_._1.equals(triplesMapId));
      if(offsetString.isDefined) {
        val offsetLong = offsetString.get._2.toLong;
        result.setOffset(offsetLong);
      }

    } catch {
      case e:Exception => {
        logger.error("errors parsing LIMIT from properties file!")
      }
    }

    //System.out.println("unfoldTriplesMap = " + result);

    result;

  }



  def unfoldTriplesMap(triplesMap:R2RMLTriplesMap , subjectURI:String ) : IQuery  = {
    val logicalTable = triplesMap.getLogicalTable().asInstanceOf[R2RMLLogicalTable];
    val subjectMap = triplesMap.subjectMap;
    val predicateObjectMaps = triplesMap.predicateObjectMaps;
    val triplesMapId = triplesMap.id;

    val resultAux = this.unfoldTriplesMap(triplesMapId, logicalTable, subjectMap, predicateObjectMaps);
    val result = if(subjectURI != null) {
      val whereExpression = MorphRDBUtility.generateCondForWellDefinedURI(
        subjectMap, triplesMap, subjectURI, logicalTable.alias);
      if(whereExpression != null) {
        resultAux.addWhere(whereExpression);
        resultAux;
      } else {
        null;
      }
    } else {
      resultAux;
    }
    result;
  }

  def unfoldTriplesMap(triplesMap:R2RMLTriplesMap ) : IQuery  = {
    this.unfoldTriplesMap(triplesMap, null);
  }

  override def unfoldConceptMapping(cm:MorphBaseClassMapping) : IQuery  = {
    this.unfoldTriplesMap(cm.asInstanceOf[R2RMLTriplesMap]);
  }

  override def unfoldConceptMapping(cm:MorphBaseClassMapping,subjectURI:String):IQuery={
    this.unfoldTriplesMap(cm.asInstanceOf[R2RMLTriplesMap], subjectURI);
  }

  override def unfoldMappingDocument() = {
    val triplesMaps = this.md.classMappings
    val result = if(triplesMaps != null) {
      triplesMaps.flatMap(triplesMap => {
        try {
          val triplesMapUnfolded = this.unfoldConceptMapping(triplesMap);
          Some(triplesMapUnfolded);
        } catch {
          case e:Exception => {
            logger.error("error while unfolding triplesMap : " + triplesMap);
            logger.error("error message = " + e.getMessage());
            None
          }
        }
      })
    } else {
      Nil
    }
    result;
  }

  override def unfoldSubject(cm:MorphBaseClassMapping) : IQuery = {
    val triplesMap = cm.asInstanceOf[R2RMLTriplesMap];
    val logicalTable = triplesMap.getLogicalTable().asInstanceOf[R2RMLLogicalTable];
    val subjectMap = triplesMap.subjectMap;
    val predicateObjectMaps = triplesMap.predicateObjectMaps;
    val id = triplesMap.id;
    val result = this.unfoldTriplesMap(id, logicalTable, subjectMap, null);
    return result;
  }

  //	def unfoldSubjectMap2(subjectMap:R2RMLSubjectMap, logicalTable:R2RMLLogicalTable):SQLQuery={
  ////		R2RMLLogicalTable logicalTable = triplesMap.getLogicalTable();
  ////		R2RMLSubjectMap subjectMap = triplesMap.getSubjectMap();
  //
  //		val result = new SQLQuery();
  //
  //
  //		val logicalTableUnfolded :SQLFromItem = logicalTable match {
  //		  case _:R2RMLTable => {
  //				this.unfoldLogicalTable(logicalTable).asInstanceOf[SQLFromItem];
  //			}
  //		  case _:R2RMLSQLQuery => {
  //				val logicalTableAux = logicalTable.accept(this);
  //				logicalTableAux match {
  //				  case _:SQLQuery => {
  //						val zQuery = this.unfoldLogicalTable(logicalTable).asInstanceOf[ZQuery];
  //						new SQLFromItem(zQuery.toString(), LogicalTableType.QUERY_STRING, this.dbType);
  //					}
  //				  case sqlFromItem:SQLFromItem => { sqlFromItem; }
  //				  case _ => { null}
  //				}
  //			}
  //		  case _ => {null}
  //		}
  //
  //		val logicalTableAlias = logicalTableUnfolded.generateAlias();
  //		logicalTable.setAlias(logicalTableAlias);
  //		//result.addFrom(logicalTableUnfolded);
  //		val logicalTableUnfoldedJoinTable = new SQLJoinTable(logicalTableUnfolded, null, null);
  //		result.addFromItem(logicalTableUnfoldedJoinTable);
  //
  //		val subjectMapSelectItems = this.unfoldTermMap(subjectMap, logicalTableAlias).toSet;
  //		val resultSelectItems : Set[ZSelectItem] = subjectMapSelectItems.toSet;
  //		result.setSelectItems(resultSelectItems);
  //		result;
  //	}

  def visit(logicalTable:R2RMLLogicalTable ) : SQLLogicalTable  ={
    val result = this.unfoldLogicalTable(logicalTable);
    result;
  }

  def visit( md:R2RMLMappingDocument) : Collection[IQuery] = {
    val  result = this.unfoldMappingDocument();
    result;
  }

  def  visit( objectMap:R2RMLObjectMap) : Object = {
    // TODO Auto-generated method stub
    null;
  }

  def visit(refObjectMap:R2RMLRefObjectMap ) : Object = {
    // TODO Auto-generated method stub
    null;
  }

  def  visit(r2rmlTermMap:R2RMLTermMap ) : Object = {
    // TODO Auto-generated method stub
    null;
  }

  def visit(triplesMap:R2RMLTriplesMap ) : IQuery  = {
    val result = this.unfoldTriplesMap(triplesMap);
    result;
  }
}

object MorphRDBUnfolder {
  def unfoldJoinConditions(pJoinConditions:Iterable[R2RMLJoinCondition]
                           , childTableAlias:String, joinQueryAlias:String , dbType:String ) : ZExpression  = {
    val joinConditions = {
      if(pJoinConditions == null) { Nil }
      else {pJoinConditions}
    }

    //var onExpression : ZExpression = null;
    val enclosedCharacter = Constants.getEnclosedCharacter(dbType);

    val joinConditionExpressions = joinConditions.map(joinCondition => {
      var childColumnName = joinCondition.childColumnName
      childColumnName = childColumnName.replaceAllLiterally("\\\"", enclosedCharacter);
      childColumnName = childTableAlias + "." + childColumnName;
      val childColumn = new ZConstant(childColumnName, ZConstant.COLUMNNAME);

      var parentColumnName = joinCondition.parentColumnName;
      parentColumnName = parentColumnName.replaceAllLiterally("\\\"", enclosedCharacter);
      parentColumnName = joinQueryAlias + "." + parentColumnName;
      val parentColumn = new ZConstant(parentColumnName, ZConstant.COLUMNNAME);

      new ZExpression("=", childColumn, parentColumn);
    })

    val result = if(joinConditionExpressions.size > 0) {
      MorphSQLUtility.combineExpresions(joinConditionExpressions, Constants.SQL_LOGICAL_OPERATOR_AND);
    } else {
      Constants.SQL_EXPRESSION_TRUE;
    }

    result;
  }

  def getReferencedColumns(termMap:R2RMLTermMap, dbType:String) : List[String] = {
    val enclosedCharacter = Constants.getEnclosedCharacter(dbType);

    val result : List[String] = if(termMap.termMapType == Constants.MorphTermMapType.ColumnTermMap) {
      //List(this.getOriginalValue());
      List(termMap.columnName.replaceAllLiterally("\\\"", enclosedCharacter));
    } else if(termMap.termMapType == Constants.MorphTermMapType.TemplateTermMap) {
      val template = termMap.getOriginalValue().replaceAllLiterally("\\\"", enclosedCharacter)
      RegexUtility.getTemplateColumns(template, true).toList;
    } else {
      Nil
    }

    result;
  }

}