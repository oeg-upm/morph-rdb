package es.upm.fi.dia.oeg.morph.rdb.querytranslator

import scala.collection.JavaConversions._
import java.sql.{Connection, ResultSet}
import java.util.regex.Matcher
import java.util.regex.Pattern

import Zql.ZConstant
import Zql.ZExp
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import es.upm.fi.dia.oeg.morph.base._
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLRefObjectMap
import es.upm.fi.dia.oeg.morph.base.querytranslator.NameGenerator
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseBetaGenerator
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseCondSQLGenerator
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseQueryTranslator
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseAlphaGenerator
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBasePRSQLGenerator
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.model.MorphBasePropertyMapping
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseResultSet
import es.upm.fi.dia.oeg.morph.base.sql.IQuery
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.base.querytranslator.engine.MorphMappingInferrer
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBNodeGenerator
import org.apache.jena.datatypes.RDFDatatype
import org.apache.jena.datatypes.xsd.XSDDatatype
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

class MorphRDBQueryTranslator(nameGenerator:NameGenerator
                              , alphaGenerator:MorphBaseAlphaGenerator, betaGenerator:MorphBaseBetaGenerator
                              , condSQLGenerator:MorphBaseCondSQLGenerator, prSQLGenerator:MorphBasePRSQLGenerator)
  extends MorphBaseQueryTranslator(nameGenerator:NameGenerator
    , alphaGenerator:MorphBaseAlphaGenerator, betaGenerator:MorphBaseBetaGenerator
    , condSQLGenerator:MorphBaseCondSQLGenerator, prSQLGenerator:MorphBasePRSQLGenerator) {



  override val logger = LoggerFactory.getLogger(this.getClass());

  this.alphaGenerator.owner = this;
  this.betaGenerator.owner = this;
  this.condSQLGenerator.owner = this;

  var mapTemplateMatcher:Map[String, Matcher] = Map.empty;
  var mapTemplateAttributes:Map[String, java.util.List[String]] = Map.empty;

  val enclosedCharacter = Constants.getEnclosedCharacter(this.databaseType);
  //val nodeGenerator = new MorphRDBNodeGenerator(this.properties)


  override def transIRI(node:Node) : List[ZExp] = {
    val cms = if(mapInferredTypes.contains(node) ){
      mapInferredTypes(node);
    } else {
      val mappingInferrer = new MorphMappingInferrer(this.mappingDocument);
      mappingInferrer.inferByURI(node.toString())
    }


    val cm = cms.iterator().next().asInstanceOf[R2RMLTriplesMap];
    val mapColumnsValues = cm.subjectMap.getTemplateValues(node.getURI());
    val result:List[ZExp] = {
      if(mapColumnsValues == null || mapColumnsValues.size() == 0) {
        //do nothing
        Nil
      } else {
        val resultAux = mapColumnsValues.keySet.map(column => {
          val value = mapColumnsValues(column);
          val constant = new ZConstant(value, ZConstant.STRING);
          constant;
        })
        resultAux.toList;
      }
    }

    result;
  }

  //	override def buildAlphaGenerator() = {
  //		val alphaGenerator = new MorphRDBAlphaGenerator(this);
  //		super.setAlphaGenerator(alphaGenerator);
  //
  //	}
  //
  //	override def buildBetaGenerator() = {
  //		val betaGenerator = new MorphRDBBetaGenerator(this);
  //		super.setBetaGenerator(betaGenerator);
  //	}
  //
  //	override def buildCondSQLGenerator() = {
  //		val condSQLGenerator = new MorphRDBCondSQLGenerator(this);
  //		super.setCondSQLGenerator(condSQLGenerator);
  //	}
  //
  //	override def buildPRSQLGenerator() = {
  //		val prSQLGenerator = new MorphRDBPRSQLGenerator(this);
  //		super.setPrSQLGenerator(prSQLGenerator);
  //	}

  def getMappedMappingByVarName(varName:String, rs:ResultSet) = {
    val mapValue = {
      try {
        val mappingHashCode = rs.getInt(Constants.PREFIX_MAPPING_ID + varName);

        //IN CASE OF UNION, A VARIABLE MAY MAPPED TO MULTIPLE MAPPINGS
        if(mappingHashCode == 0) {
          val varNameHashCode = varName.hashCode();
          //super.getMappedMapping(varNameHashCode);
          this.prSQLGenerator.getMappedMapping(varNameHashCode)
        } else {
          //super.getMappedMapping(mappingHashCode);
          this.prSQLGenerator.getMappedMapping(mappingHashCode)
        }
      } catch {
        case e:Exception => {
          None
        }
      }
    }

    mapValue;
  }

  override def generateNode(rs:ResultSet, varName:String, mapXSDDatatype:Map[String, String]
                            , varNameColumnLabels:List[String]
                           ) : Node = {
    val result:Node = {
      try {
        if(rs != null) {
          //val rsColumnNames = rs.getColumnNames();
          //val columnNames = CollectionUtility.getElementsStartWith(rsColumnNames, varName + "_");
          //val columnNames = CollectionUtility.getElementsStartWith(rsColumnNames, varName);

          val mapValue = this.getMappedMappingByVarName(varName, rs);

          if(!mapValue.isDefined) {
            val originalValue = rs.getString(varName);
            val node = if(originalValue == null ) { null }
            else { NodeFactory.createLiteral(originalValue); }
            //new TermMapResult(node, originalValue, null,None)
            //new TranslatedValue(node, List(originalValue))
            node
          } else {
            val termMap : R2RMLTermMap = {
              mapValue.get match {
                case mappedValueTermMap:R2RMLTermMap => {
                  mappedValueTermMap;
                }
                case mappedValueRefObjectMap:R2RMLRefObjectMap => {
                  //						    val parentTriplesMap = mappedValueRefObjectMap.getParentTriplesMap().asInstanceOf[R2RMLTriplesMap];
                  val md = this.mappingDocument.asInstanceOf[R2RMLMappingDocument];
                  val parentTriplesMap = md.getParentTriplesMap(mappedValueRefObjectMap);
                  parentTriplesMap.subjectMap;
                }
                case _ => {
                  logger.debug("undefined type of mapping!");
                  null
                }
              }
            }

            this.generateNode(rs, termMap, mapXSDDatatype, varName, varNameColumnLabels);

            /*
            val termMapResult = {
              if(resultAux != null) {
                if(termMapType != null) {
                  if(termMapType.equals(Constants.R2RML_IRI_URI)) {
                    val uri =GeneralUtility.encodeURI(resultAux, properties.mapURIEncodingChars
                      , properties.uriTransformationOperation);
                    val node = NodeFactory.createURI(uri);
                    node
                  } else if(termMapType.equals(Constants.R2RML_LITERAL_URI)) {
                    val literalValue = GeneralUtility.encodeLiteral(resultAux);
                    val xsdDatatype = termMap.datatype;
                    val node = if(xsdDatatype == null || xsdDatatype.isEmpty) {
                      NodeFactory.createLiteral(literalValue);
                    } else {
                      val rdfDataType = new XSDDatatype(xsdDatatype.get)
                      NodeFactory.createLiteral(literalValue, rdfDataType);
                    }
                    node
                  } else {
                    val node = NodeFactory.createLiteral(resultAux);
                    node
                  }
                } else {
                  val node = NodeFactory.createLiteral(resultAux);
                  node
                }
              } else {
                null
              }
            }
            termMapResult
            */

            /*
            if(dbValue != null) {
              val nodeValue = dbValue.toString
              val node = MorphRDBResultSetTranslator.generateNode(nodeValue, termMap, datatype);
              node
            } else {
              null
            }
            */
          }
        } else {
          null
        }
      } catch {
        case e:Exception => {
          e.printStackTrace();
          logger.error("Error occured while translating result set : " + e.getMessage());
          null;
        }
      }
    }

    result;
  }


  //	override def transTP(tp:Triple , cm:MorphBaseClassMapping ,predicateURI:String
  //	    , pm:MorphBasePropertyMapping ) : IQuery = {
  //		// TODO Auto-generated method stub
  //		null;
  //	}

  //	override def getTripleAlias(tp:Triple ) : String = {
  //	  if(this.mapTripleAlias.contains(tp)) {
  //	    this.mapTripleAlias(tp);
  //	  } else {
  //	    null
  //	  }
  //	}
  //
  //	override def putTripleAlias(tp:Triple , alias:String ) = {
  //		this.mapTripleAlias += (tp -> alias);
  //	}



  //def getMappingDocument(): es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument = ???
  //def getOptimizer(): es.upm.fi.dia.oeg.obdi.core.engine.IQueryTranslationOptimizer = ???



  //def setConnection(x$1: java.sql.Connection): Unit = ???

  //def setIgnoreRDFTypeStatement(x$1: Boolean): Unit = ???


  //def setUnfolder(x$1: es.upm.fi.dia.oeg.obdi.core.engine.AbstractUnfolder): Unit = ???

  def generateNode(rs:ResultSet, termMap:R2RMLTermMap, mapXSDDatatype:Map[String, String]
                   , varName:String, varNameColumnLabels:List[String]
                  ):Node= {
    val nodeGenerator = new MorphRDBNodeGenerator(this.properties)

    val node = {
      if(termMap != null) {
        termMap.termMapType match {
          case Constants.MorphTermMapType.ConstantTermMap => {
            nodeGenerator.generateNodeFromConstantMap(termMap);
          }
          case Constants.MorphTermMapType.ColumnTermMap => {
            /*
            val rsObjectVarName = rs.getObject(varName);
            if(rsObjectVarName == null) { null } else { rsObjectVarName.toString(); }
            */

            //val dbValueAux = MorphRDBResultSetTranslator.getResultSetValue(rs, varName, this.databaseType);
            //dbValueAux

            nodeGenerator.generateNodeFromColumnMap(termMap, rs, mapXSDDatatype, varName);
          }
          case Constants.MorphTermMapType.TemplateTermMap => {
            /*
            val datatype = if(termMap.datatype.isDefined) { termMap.datatype } else { None }

            //val templateString = termMap.getTemplateString();
            val templateString = termMap.getOriginalValue().replaceAllLiterally("\\\"", enclosedCharacter)

            if(this.mapTemplateMatcher.contains(templateString)) {
              val matcher = this.mapTemplateMatcher.get(templateString);
            } else {
              val pattern = Pattern.compile(Constants.R2RML_TEMPLATE_PATTERN);
              val matcher = pattern.matcher(templateString);
              this.mapTemplateMatcher += (templateString -> matcher);
            }

            val templateAttributes = {
              if(this.mapTemplateAttributes.contains(templateString)) {
                this.mapTemplateAttributes(templateString);
              } else {
                val templateAttributesAux = RegexUtility.getTemplateColumns(templateString, true);
                this.mapTemplateAttributes += (templateString -> templateAttributesAux);
                templateAttributesAux;
              }
            }

            var i = 0;
            val replacements:Map[String, String] = templateAttributes.flatMap(templateAttribute => {
              val columnName = {
                if(columnNames == null || columnNames.isEmpty()) {
                  varName;
                } else {
                  varName + "_" + i;
                }
              }
              i = i + 1;

              val dbValue = rs.getString(columnName);
              Some(templateAttribute -> dbValue);
            }).toMap

            val node = if(replacements.isEmpty) {
              null
            } else {
              val templateResult = RegexUtility.replaceTokens(templateString, replacements);
              if(templateResult != null) {
                MorphRDBResultSetTranslator.generateNode(templateResult, termMap, datatype);
              } else {
                null
              }
            }
            new TranslatedValue(node, null)
            */

            val dbType = this.properties.databaseType;
            val dbEnclosedCharacter = Constants.getEnclosedCharacter(dbType);
            val termMapTemplateString = termMap.templateString.replaceAllLiterally("\\\"", dbEnclosedCharacter);
            val attributes = RegexUtility.getTemplateColumns(termMapTemplateString, true);
            var i = 0;
            val mapTemplateColumns:Map[String, String] = attributes.map(attribute => {
              val databaseColumn = if(varName != null ) {
                val columnName = if(varNameColumnLabels == null || varNameColumnLabels.isEmpty()) {
                  varName;
                } else {
                  varName + "_" + i;
                }
                i = i + 1;
                columnName
              } else { attribute; }
              (attribute -> databaseColumn)
            }).toMap

            //nodeGenerator.generateNodeFromTemplateMap(termMap, rs, null, varName, varNameColumnLabels);
            nodeGenerator.generateNodeFromTemplateMap(termMap, rs, mapTemplateColumns);
          }


          case _ => {
            logger.debug("Unsupported term map type!");
            null;
          }
        }
      } else {
        null;
      }
    }
    node


  }
}

//}