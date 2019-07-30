package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import java.sql.ResultSet
import java.util.Properties

import Zql.ZConstant
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLConstant
import es.upm.fi.dia.oeg.morph.base._
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap
import org.apache.jena.graph.NodeFactory
import org.apache.jena.rdf.model.AnonId
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

class MorphRDBResultSetTranslator {

}

object MorphRDBResultSetTranslator {
  val logger = LoggerFactory.getLogger(this.getClass)

  def getResultSetValue(rs:ResultSet
                        //, termMapDatatype:Option[String]
                        , pColumnName:String
                        , dbType:String
                       ) : String = {
    try {
      val dbEnclosedCharacter = Constants.getEnclosedCharacter(dbType);

      //val logicalTableMetaData = ownerTriplesMap.getLogicalTable();

      //val dbType = this.configurationProperties.databaseType;
      //val dbType = logicalTableMetaData.getTableMetaData.dbType;
      //val zConstant = MorphSQLConstant(pColumnName, ZConstant.COLUMNNAME, dbType);
      val zConstant = MorphSQLConstant(pColumnName, ZConstant.COLUMNNAME);
      val tableName = zConstant.table;
      //val columnNameAux = zConstant.column.replaceAll("\"", "")
      val columnNameAux = zConstant.column.replaceAll(dbEnclosedCharacter, ""); //doesn't work for 9a
      //val columnNameAux = zConstant.column


      val columnName = {
        if(tableName != null) {
          tableName + "." + columnNameAux
        } else {
          columnNameAux
        }
      }


      /*
      val result = if(termMapDatatype == null) {
        rs.getString(columnName);
      } else if(!termMapDatatype.isDefined) {
        rs.getString(columnName);
      }
      //			else if(termMapDatatype.get.equals(XSDDatatype.XSDdateTime.getURI())) {
      //				val rsDateValue = rs.getDate(columnName);
      //				if(rsDateValue == null) { null; } else { rsDateValue.toString(); }
      //			}
      else {
        rs.getObject(columnName);
      }
      */

      val result = rs.getString(columnName);
      result
    } catch {
      case e:Exception => {
        e.printStackTrace();
        logger.error("error occured when translating result: " + e.getMessage());
        null
      }
    }
  }

  def generateNode(nodeValue:String
                   , termMap:R2RMLTermMap
                   , datatype:Option[String]
                   //                   , termMapType:String
                   //                       , mapXMLDatatype : Map[String, String]
                  ) = {
    val result = if(nodeValue != null) {
      //val termMapType = termMap.inferTermType;
      termMap.inferTermType match {
        case Constants.R2RML_IRI_URI => {
          NodeFactory.createURI(nodeValue);
        }
        case Constants.R2RML_LITERAL_URI => {

          /*
          val datatype = if(termMap.datatype.isDefined) { termMap.datatype }
          else {
            val datatypeAux = {
              val columnNameAux = termMap.columnName.replaceAll("\"", "");
              if(mapXMLDatatype != null) {
                val columnNameAuxDatatype = mapXMLDatatype.get(columnNameAux);
                if(columnNameAuxDatatype != None) { columnNameAuxDatatype }
                else { mapXMLDatatype.get(columnTermMapValue); }
              } else {
                None
              }
            }
            datatypeAux
          }
          */

          //this.createLiteral(nodeValue, datatype, termMap.languageTag);
          if(termMap.languageTag == null || termMap.languageTag.isEmpty) {
            if(datatype == null || datatype.isEmpty) {
              NodeFactory.createLiteral(nodeValue)
            } else {
              val rdfDataType = GeneralUtility.getXSDDatatype(datatype.get)
              NodeFactory.createLiteral(nodeValue, rdfDataType);
            }
          } else {
            if(datatype == null || datatype.isEmpty) {
              NodeFactory.createLiteral(nodeValue, termMap.languageTag.get)
            } else {
              val rdfDataType = GeneralUtility.getXSDDatatype(datatype.get)
              NodeFactory.createLiteral(nodeValue, termMap.languageTag.get, rdfDataType);
            }
          }


        }
        case Constants.R2RML_BLANKNODE_URI => {
          val anonId = new AnonId(nodeValue.toString());
          //this.materializer.model.createResource(anonId)
          NodeFactory.createBlankNode(anonId.getLabelString)
        }
        case _ => {
          null
        }
      }
    } else {
      null
    }
    result
  }

  def generateNodeFromConstantMap(termMap:R2RMLTermMap) = {
    val datatype = if(termMap.datatype.isDefined) { termMap.datatype } else { None }
    val node = MorphRDBResultSetTranslator.generateNode(termMap.constantValue, termMap, datatype)
    new TranslatedValue(node, List());
  }

  def generateNodeFromColumnMap(termMap:R2RMLTermMap, rs:ResultSet
                                //, logicalTableAlias:String
                                , dbType:String
                                , mapXMLDatatype : Map[String, String]
                                , columnName:String
                               ) = {

    val datatype = if(termMap.datatype.isDefined) { termMap.datatype }
    else {
      val columnNameAux = termMap.columnName.replaceAll("\"", "");
      val datatypeAux = {
        if(mapXMLDatatype == null || mapXMLDatatype.isEmpty) {
          null
        } else {
          val columnNameAuxDatatype = mapXMLDatatype.get(columnNameAux);
          if(columnNameAuxDatatype.isDefined) { columnNameAuxDatatype }
          else { mapXMLDatatype.get(columnName); }
        }
      }
      datatypeAux
    }

    val dbValueAux = this.getResultSetValue(rs, columnName, dbType);
    //			  val dbValue = dbValueAux match {
    //				  case dbValueAuxString:String => {
    //					  if(this.properties.transformString.isDefined) {
    //					    this.properties.transformString.get match {
    //						    case Constants.TRANSFORMATION_STRING_TOLOWERCASE => {
    //						      dbValueAuxString.toLowerCase();
    //						    }
    //						    case Constants.TRANSFORMATION_STRING_TOUPPERCASE => {
    //						      dbValueAuxString.toUpperCase();
    //						    }
    //						    case _ => { dbValueAuxString }
    //					    }
    //
    //					  }
    //					  else { dbValueAuxString }
    //				  }
    //				  case _ => { dbValueAux }
    //			  }
    //val dbValue = dbValueAux;



    //val result = (this.generateNode(dbValue, termMap, datatype), List(dbValue));

    val dbValue  = if(Constants.DATABASE_H2_NULL_VALUE.equals(dbValueAux)
      && Constants.DATABASE_CSV.equals(dbType)) {
      null
    } else {
      dbValueAux
    }

    val node = if(dbValue != null) {
      MorphRDBResultSetTranslator.generateNode(dbValue.toString, termMap, datatype);
    } else {
      null
    }

    new TranslatedValue(node, List(dbValue));
  }



  def generateNodeFromTemplateMap(termMap:R2RMLTermMap, rs:ResultSet, dbType:String
                                  , properties:MorphProperties, logicalTableAlias:String
                                  , varName:String, varColumnNames:List[String]
                                 ) = {
    val dbEnclosedCharacter = Constants.getEnclosedCharacter(dbType);

    val datatype = if(termMap.datatype.isDefined) { termMap.datatype } else { None }
    var rawDBValues:List[Object] = Nil;
    val termMapTemplateString = termMap.templateString.replaceAllLiterally("\\\"", dbEnclosedCharacter);

    val attributes = RegexUtility.getTemplateColumns(termMapTemplateString, true);
    var i = 0;
    val replacements:Map[String, String] = attributes.flatMap(attribute => {
      val databaseColumn = if(logicalTableAlias != null) {
        val attributeSplit = attribute.split("\\.");
        if(attributeSplit.length >= 1) {
          val columnName = attributeSplit(attributeSplit.length - 1).replaceAll("\"", dbEnclosedCharacter);
          logicalTableAlias + "_" + columnName;
        }
        else { logicalTableAlias + "_" + attribute; }
      } else if(varName != null ) {
        val columnName = if(varColumnNames == null || varColumnNames.isEmpty()) {
          varName;
        } else {
          varName + "_" + i;
        }
        i = i + 1;
        columnName
      } else { attribute; }

      val dbValueAux = MorphRDBResultSetTranslator.getResultSetValue(rs, databaseColumn, dbType);
      //logger.info(s"dbValueAux = ${dbValueAux}")


      if(dbValueAux != null) {
        rawDBValues = rawDBValues ::: List(dbValueAux);
      }


      val dbValue = dbValueAux match {
        case dbValueAuxString:String => {
          if(properties.transformString.isDefined) {
            properties.transformString.get match {
              case Constants.TRANSFORMATION_STRING_TOLOWERCASE => {
                dbValueAuxString.toLowerCase();
              }
              case Constants.TRANSFORMATION_STRING_TOUPPERCASE => {
                dbValueAuxString.toUpperCase();
              }
              case _ => { dbValueAuxString }
            }

          }
          else { dbValueAuxString }
        }
        case _ => { dbValueAux }
      }
      //logger.info(s"dbValue = ${dbValue}")


      if(dbValue != null) {
        var databaseValueString = dbValue.toString();
        if(termMap.inferTermType.equals(Constants.R2RML_IRI_URI)) {
          val uriTransformationOperations = properties.uriTransformationOperation;
          if(uriTransformationOperations != null) {
            uriTransformationOperations.foreach{
              case Constants.URI_TRANSFORM_TOLOWERCASE => {
                databaseValueString = databaseValueString.toLowerCase();
              }
              case Constants.URI_TRANSFORM_TOUPPERCASE => {
                databaseValueString = databaseValueString.toUpperCase();
              }
              case _ => { }
            }
          }
        }

        Some(attribute -> databaseValueString);
      } else {
        None
        //Some(attribute -> "null");
      }
    }).toMap

    //logger.info(s"replacements = ${replacements}")
    //logger.info(s"termMapTemplateString = ${termMapTemplateString}")
    val node = if(replacements.isEmpty) {
      //(this.translateData(termMap, termMapTemplateString, datatype), rawDBValues);
      null
    } else {
      val templateWithDBValue = RegexUtility.replaceTokens(termMapTemplateString, replacements);
      if(templateWithDBValue != null) {
        MorphRDBResultSetTranslator.generateNode(templateWithDBValue, termMap, datatype);
      } else {
        null
      }
    }
    new TranslatedValue(node, rawDBValues);
  }
}