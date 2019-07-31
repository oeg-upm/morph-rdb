package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import java.util.Collection

import es.upm.fi.dia.oeg.morph.base.Constants
import Zql.ZExpression

import scala.collection.JavaConversions._
import Zql.ZConstant
import Zql.ZQuery
import java.io.ByteArrayInputStream

import Zql.ZqlParser
import es.upm.fi.dia.oeg.morph.base.sql._
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap
import java.sql.{Connection, ResultSet}

import org.apache.commons.io.FilenameUtils
import java.io.File

import es.upm.fi.dia.oeg.morph.base.GitHubUtility
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

class MorphRDBUtility {

}

object MorphRDBUtility {
  val logger = LoggerFactory.getLogger(this.getClass());

  def generateCondForWellDefinedURI(termMap:R2RMLTermMap
                                    , ownerTriplesMap:MorphBaseClassMapping, uri:String , alias:String
                                   ) : ZExpression = {
    val logicalTable = ownerTriplesMap.getLogicalTable();
    val logicalTableMetaData = logicalTable.tableMetaData;
    //val tableMetaData = logicalTableMetaData;
    val dbType = if(logicalTableMetaData.isDefined) { logicalTableMetaData.get.dbType }
    else { Constants.DATABASE_DEFAULT }

    //			val tableMetaData = {
    //					if(logicalTableMetaData == null && conn != null) {
    //						try {
    //							logicalTable.buildMetaData(conn);
    //							logicalTable.getTableMetaData();
    //						} catch {
    //						case e:Exception => {
    //							logger.error(e.getMessage());
    //							throw new Exception(e.getMessage());
    //						}
    //						}
    //					} else {
    //						logicalTableMetaData
    //					}
    //			}

    val result:ZExpression = {
      if(termMap.termMapType == Constants.MorphTermMapType.TemplateTermMap) {
        val matchedColValues = termMap.getTemplateValues(uri);
        if(matchedColValues == null || matchedColValues.size == 0) {
          val errorMessage = "uri " + uri + " doesn't match the template : " + termMap.templateString;
          logger.debug(errorMessage);
          null
        } else {
          val exprs:List[ZExpression] = {
            val exprsAux = matchedColValues.keySet.map(pkColumnString => {
              val value = matchedColValues(pkColumnString);

              //									val termMapColumnTypeName = termMap.columnTypeName;
              //									val columnTypeName = {
              //											if(termMapColumnTypeName != null) {
              //												termMapColumnTypeName
              //											} else {
              //												if(tableMetaData != null && tableMetaData.getColumnMetaData(pkColumnString).isDefined) {
              //													val columnTypeNameAux = tableMetaData.getColumnMetaData(pkColumnString).get.dataType;
              //													termMap.columnTypeName = columnTypeNameAux;
              //													columnTypeNameAux
              //												} else {
              //													null
              //												}
              //											}
              //									}



              val pkColumnConstant = MorphSQLConstant.apply(
                alias + "." + pkColumnString
                , ZConstant.COLUMNNAME, dbType);

              //									val columnTypeName = null;
              //									val pkValueConstant = {
              //											if(columnTypeName != null) {
              //
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
              val pkValueConstant = new ZConstant(value, ZConstant.STRING);

              val expr = new ZExpression("=", pkColumnConstant, pkValueConstant);
              expr;
            })
            exprsAux.toList;
          }

          MorphSQLUtility.combineExpresions(
            exprs, Constants.SQL_LOGICAL_OPERATOR_AND);
        }
      } else {
        null
      }
    }

    logger.debug("generateCondForWellDefinedURI = " + result);
    result;
  }

  //	def generateJoinCondition(joinConditions:Collection[R2RMLJoinCondition]
  //	, parentTableAlias:String, joinQueryAlias:String , dbType:String ) : ZExpression = {
  //		var onExpression:ZExpression = null;
  //		val enclosedCharacter = Constants.getEnclosedCharacter(dbType);
  //
  //		if(joinConditions != null) {
  //			for(joinCondition <- joinConditions) {
  //				var childColumnName = joinCondition.getChildColumnName();
  //				childColumnName = childColumnName.replaceAll("\"", enclosedCharacter);
  //				childColumnName = parentTableAlias + "." + childColumnName;
  //				val childColumn = new ZConstant(childColumnName, ZConstant.COLUMNNAME);
  //
  //				var parentColumnName = joinCondition.getParentColumnName();
  //				parentColumnName = parentColumnName.replaceAll("\"", enclosedCharacter);
  //				parentColumnName = joinQueryAlias + "." + parentColumnName;
  //				val parentColumn = new ZConstant(parentColumnName, ZConstant.COLUMNNAME);
  //
  //				val joinConditionExpression = new ZExpression("=", childColumn, parentColumn);
  //				if(onExpression == null) {
  //					onExpression = joinConditionExpression;
  //				} else {
  //					onExpression = new ZExpression("AND", onExpression, joinConditionExpression);
  //				}
  //			}
  //		}
  //
  //		return onExpression;
  //	}

  def toZQuery(sqlString:String ) : ZQuery = {
    try {
      //sqlString = sqlString.replaceAll(".date ", ".date2");
      val bs = new ByteArrayInputStream(sqlString.getBytes());
      val parser = new ZqlParser(bs);
      val statement = parser.readStatement();
      val zQuery = statement.asInstanceOf[ZQuery];
      zQuery;
    } catch {
      case e:Exception => {
        val errorMessage = "error parsing query string : \n" + sqlString;
        logger.debug(errorMessage);
        logger.debug("error message = " + e.getMessage());
        throw e;
      }
      case e:Error => {
        val errorMessage = "error parsing query string : \n" + sqlString;
        logger.error(errorMessage);
        throw new Exception(errorMessage);
      }
    }
  }

  def toSQLQuery(sqlString:String ) : SQLQuery = {
    val zQuery = this.toZQuery(sqlString);
    val sqlQuery = new SQLQuery(zQuery);
    sqlQuery;
  }

  def loadCSVFromURL(url:String) : File = {
    val f = new File(url);
    f;
  }

  def loadCSVFromGitHubBlob(blobURL:String) : File = {
    logger.info("Loading CSV file from github:" + blobURL);
    val rawURL = GitHubUtility.getRawURLFromBlobURL(blobURL);
    this.loadCSVFromURL(rawURL);
  }

  def loadCSVFromLocalFile(localPath:String) : File = {
    logger.info("Loading CSV file: " + localPath);
    this.loadCSVFromURL(localPath);
  }

  def loadCSVFromLocation(location:String) : File = {
    logger.info("Loading CSV file ...");

    if(location.contains("https://github.com") && location.contains("/blob/")) {
      this.loadCSVFromGitHubBlob(location);
    } else {
      this.loadCSVFromLocalFile(location);
    }
  }


  def loadCSVFile(conn:Connection , csv_file:String) : Unit = {
    this.loadCSVFile(conn, csv_file, None);
  }

  def loadCSVFile(conn:Connection , pCSVFile:String, fieldSeparator:Option[String]) : Unit = {
    //String csv_file_extension = "";
    if(pCSVFile == null) {
      throw new Exception("CSV file has not been defined.");
    }

    val fileURL = if(pCSVFile.contains("https://github.com") && pCSVFile.contains("/blob/")) {
      GitHubUtility.getRawURLFromBlobURL(pCSVFile);
    } else {
      pCSVFile
    }

    //val f = new File(csv_file);
    val f = this.loadCSVFromLocation(fileURL);
    val filename = f.getName;

    val lastDotChar = filename.lastIndexOf(".");
    if(lastDotChar == -1) {
      throw new Exception("CSV file does not have any extension.");
    }
    val tableName = filename.substring(0, lastDotChar).toUpperCase();

    val dropTableString = "DROP TABLE \"" + tableName + "\"";

    val createTableString = if(fieldSeparator.isDefined) {
      logger.info("Field separator = " + fieldSeparator.get);
      "CREATE TABLE \"" + tableName + "\" AS SELECT * FROM CSVREAD('" + fileURL + "', NULL, 'fieldSeparator=" + fieldSeparator.get +"');";
    } else {
      "CREATE TABLE \"" + tableName + "\" AS SELECT * FROM CSVREAD('" + fileURL + "');";
    }
    logger.info(s"createTableString = ${createTableString}");

    val stmt = conn.createStatement();

    try {
      //logger.debug("dropTableString = " + dropTableString);
      stmt.execute(dropTableString);
      conn.commit();
      logger.info("The table:" + tableName  + " was dropped successfully");
    } catch {
      case sqle:Exception => {
        logger.error("Error while dropping the table: " + tableName  + " : " + sqle.getMessage);
        //sqle.printStackTrace();
      }
    }

    try {
      logger.debug("createTableString = " + createTableString);
      stmt.execute(createTableString);
      logger.info("Table " + tableName  + " was created successfully");
    } catch {
      case sqle:Exception => {
        logger.error("Error while creating the table: " + tableName  + " : " + sqle.getMessage);
        //sqle.printStackTrace();
      }
    }

    conn.commit();
  }


}