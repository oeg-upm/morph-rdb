package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import scala.collection.JavaConversions._
import es.upm.fi.dia.oeg.morph.base._
import java.util.Collection
import java.sql.ResultSet

import org.apache.jena.datatypes.xsd.XSDDatatype
import java.sql.ResultSetMetaData
import java.sql.Connection

import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLPredicateObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLRefObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLConstant
import Zql.ZConstant
import es.upm.fi.dia.oeg.morph.base.sql.DatatypeMapper
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLUtility
import es.upm.fi.dia.oeg.morph.base.sql.IQuery
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLLogicalTable
import org.apache.jena.rdf.model.RDFNode
import org.apache.jena.rdf.model.AnonId
import org.apache.jena.vocabulary.RDF
import org.apache.jena.rdf.model.Literal
import es.upm.fi.dia.oeg.morph.base.materializer.MorphBaseMaterializer
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.MorphR2RMLElementVisitor
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataTranslator
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseDataSourceReader
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLSubjectMap
import org.apache.jena.rdf.model.Property
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLPredicateMap
import java.text.SimpleDateFormat
import java.text.DateFormat
import java.util.Locale

import org.apache.jena.graph.{Node, NodeFactory}
import org.slf4j.LoggerFactory

class MorphRDBDataTranslator(md:R2RMLMappingDocument, materializer:MorphBaseMaterializer
                             , unfolder:MorphRDBUnfolder, dataSourceReader:MorphRDBDataSourceReader
                             , connection:Connection, properties:MorphProperties)
  extends MorphBaseDataTranslator(md, materializer , unfolder, dataSourceReader
    , connection, properties)
    with MorphR2RMLElementVisitor {
  val dfInput = this.properties.inputDateFormat;
  //val dfOutput = this.properties.outputDateFormat;
  val xsdDateTimeURI = XSDDatatype.XSDdateTime.getURI().toString();
  val xsdBooleanURI = XSDDatatype.XSDboolean.getURI().toString();
  val xsdDurationURI = XSDDatatype.XSDduration.getURI().toString();
  val xsdDateURI = XSDDatatype.XSDdate.getURI().toString();
  val dbType = this.properties.databaseType;
  override val logger = LoggerFactory.getLogger(this.getClass());
  val nodeGenerator = new MorphRDBNodeGenerator(this.properties)


  override def processCustomFunctionTransformationExpression(
                                                              argument:Object ) : Object = {
    null;
  }

  override def translateData(triplesMap:MorphBaseClassMapping) : Unit = {
    val query = this.unfolder.unfoldConceptMapping(triplesMap);
    this.generateRDFTriples(triplesMap, query);
    //		null;
  }

  override def translateData(triplesMaps:Iterable[MorphBaseClassMapping]) : Unit = {
    for(triplesMap <- triplesMaps) {
      try {
        this.visit(triplesMap.asInstanceOf[R2RMLTriplesMap]);
        //triplesMap.asInstanceOf[R2RMLTriplesMap].accept(this);
      } catch {
        case e:Exception => {
          logger.error("error while translating data of triplesMap : " + triplesMap);
          if(e.getMessage() != null) {
            logger.error("error message = " + e.getMessage());
          }

          //e.printStackTrace();
          throw new Exception(e.getMessage(), e);
        }
      }
    }
  }

  override def translateData(mappingDocument:MorphBaseMappingDocument ) = {
    val conn = this.connection

    val triplesMaps = mappingDocument.classMappings
    if(triplesMaps != null) {
      this.translateData(triplesMaps);
      //DBUtility.closeConnection(conn, "R2RMLDataTranslator");
    }
  }


  def visit( logicalTable:R2RMLLogicalTable) : Object = {
    // TODO Auto-generated method stub
    null;
  }

  override def visit(mappingDocument:R2RMLMappingDocument) : Object = {
    try {
      this.translateData(mappingDocument);
    } catch {
      case e:Exception => {
        e.printStackTrace();
        logger.error("error during data translation process : " + e.getMessage());
        throw new Exception(e.getMessage());
      }
    }

    null;
  }

  def visit(objectMap:R2RMLObjectMap ) : Object = {
    // TODO Auto-generated method stub
    null;
  }

  def visit(refObjectMap:R2RMLRefObjectMap ) : Object  = {
    // TODO Auto-generated method stub
    null;
  }

  def visit(r2rmlTermMap:R2RMLTermMap) : Object = {
    // TODO Auto-generated method stub
    null;
  }



  def generateRDFTriples(logicalTable:R2RMLLogicalTable ,  sm:R2RMLSubjectMap
                         , poms:Iterable[R2RMLPredicateObjectMap] , iQuery:IQuery) = {
    logger.info("Translating RDB data into RDF instances...");

    if(sm == null) {
      val errorMessage = "No SubjectMap is defined";
      logger.error(errorMessage);
      throw new Exception(errorMessage);
    }

    val logicalTableAlias = logicalTable.alias;

    val conn = this.connection
    val timeout = this.properties.databaseTimeout;
    val sqlQuery = iQuery.toString();
    val rows = DBUtility.execute(conn, sqlQuery, timeout);

    var mapXMLDatatype : Map[String, String] = Map.empty;
    var mapDBDatatype:Map[String, Integer]  = Map.empty;
    var rsmd : ResultSetMetaData = null;
    val datatypeMapper = new DatatypeMapper();

    try {
      rsmd = rows.getMetaData();
      val columnCount = rsmd.getColumnCount();
      for (i <- 0 until columnCount) {
        val columnName = rsmd.getColumnName(i+1);
        val columnType= rsmd.getColumnType(i+1);
        mapDBDatatype += (columnName -> new Integer(columnType));
        val mappedDatatype = datatypeMapper.getMappedType(columnType);
        //				if(mappedDatatype == null) {
        //					mappedDatatype = XSDDatatype.XSDstring.getURI();
        //				}
        if(mappedDatatype != null) {
          mapXMLDatatype += (columnName -> mappedDatatype);
        }
      }
    } catch {
      case e:Exception => {
        //e.printStackTrace();
        logger.warn("Unable to detect database columns!");
      }
    }

    val classes = sm.classURIs;
    val sgm = sm.graphMaps;

    var i=0;
    //var setSubjects : Set[RDFNode] = Set.empty;
    //var setBetaSub : Set[String] = Set.empty;

    var noOfErrors=0;
    while(rows.next()) {
      try {
        //translate subject map
        val subjectNode = this.generateNode(rows, sm, mapXMLDatatype, logicalTableAlias);
        if(subjectNode == null) {
          val errorMessage = "null value in the subject triple!";
          logger.debug("null value in the subject triple!");
          throw new Exception(errorMessage);
        }
        //setSubjects = setSubjects + subject._1
        //setBetaSub = setBetaSub + subject._2.toString
        //				val subjectString = subject.toString();
        //				this.materializer.createSubject(sm.isBlankNode(), subjectString);

        val subjectGraphs = sgm.flatMap(sgmElement=> {
          val subjectGraphNode = this.generateNode(rows, sgmElement, mapXMLDatatype, logicalTableAlias);
          //					val subjectGraphValue = this.translateData(sgmElement, unfoldedSubjectGraph, mapXMLDatatype);
          val graphMapTermType = sgmElement.inferTermType;
          val subjectGraph = graphMapTermType match {
            case Constants.R2RML_IRI_URI => {
              subjectGraphNode
            }
            case _ => {
              val errorMessage = "GraphMap's TermType is not valid: " + graphMapTermType;
              logger.warn(errorMessage);
              throw new Exception(errorMessage);
            }
          }
          if(subjectGraph == null) { None }
          else { Some(subjectGraph); }
        });


        //rdf:type
        classes.foreach(classURI => {
          //val statementObject = this.materializer.model.createResource(classURI);
          val statementObject = NodeFactory.createURI(classURI);
          if(subjectGraphs == null || subjectGraphs.isEmpty) {
            //						this.materializer.materializeRDFTypeTriple(subjectString, classURI, sm.isBlankNode(), null);
            this.materializer.materializeQuad(subjectNode, RDF.`type`.asNode(), statementObject, null);
            this.materializer.writer.flush();
          } else {
            subjectGraphs.foreach(subjectGraph => {
              //							this.materializer.materializeRDFTypeTriple(subjectString, classURI, sm.isBlankNode(), subjectGraph);
              this.materializer.materializeQuad(subjectNode, RDF.`type`.asNode(), statementObject, subjectGraph);
            });
          }
        });

        //translate predicate object map
        poms.foreach(pom => {
          val alias = if(pom.getAlias() == null) { logicalTableAlias; }
          else { pom.getAlias() }

          val predicates = pom.predicateMaps.flatMap(predicateMap => {
            val predicateNode = this.generateNode(rows, predicateMap, mapXMLDatatype, null);
            //						val predicateValue = this.translateData(predicateMap, unfoldedPredicateMap, mapXMLDatatype);
            if(predicateNode == null) { None }
            else { Some(predicateNode); }
          });

          val objects = pom.objectMaps.flatMap(objectMap => {
            val objectNode = this.generateNode(rows, objectMap, mapXMLDatatype, alias);
            //						val objectValue = this.translateData(objectMap, unfoldedObjectMap, mapXMLDatatype);
            if(objectNode == null) { None }
            else { Some(objectNode); }
          });

          val refObjects = pom.refObjectMaps.flatMap(refObjectMap => {
            val parentTripleMapName = refObjectMap.getParentTripleMapName;
            val parentTriplesMap = this.md.getParentTriplesMap(refObjectMap)
            val parentSubjectMap = parentTriplesMap.subjectMap;
            val parentTableAlias = this.unfolder.mapRefObjectMapAlias.getOrElse(refObjectMap, null);
            val parentSubjectNode = this.generateNode(rows, parentSubjectMap, mapXMLDatatype, parentTableAlias)
            //logger.info(s"parentSubjects = ${parentSubjects}")
            if(parentSubjectNode == null) { None }
            else { Some(parentSubjectNode) }
          })

          val pogm = pom.graphMaps;
          val predicateObjectGraphs = pogm.flatMap(pogmElement=> {
            val poGraphNode = this.generateNode(rows, pogmElement, mapXMLDatatype, null);
            //					  val poGraphValue = this.translateData(pogmElement, unfoldedPOGraphMap, mapXMLDatatype);
            if(poGraphNode == null) { None }
            else { Some(poGraphNode); }
          });


          if(sgm.isEmpty && pogm.isEmpty) {
            predicates.foreach(predicatesElement => {
              val quadSubject = subjectNode;
              val predicateNode = predicatesElement;

              val quadGraph = null;
              objects.foreach(objectsElement => {
                val quadObject = objectsElement;

                this.materializer.materializeQuad(quadSubject, predicateNode, quadObject, quadGraph)
              });

              refObjects.foreach(refObjectsElement => {
                this.materializer.materializeQuad(quadSubject, predicateNode, refObjectsElement, quadGraph)
              });
            });
          } else {
            val unionGraphs = subjectGraphs ++ predicateObjectGraphs
            unionGraphs.foreach(unionGraph => {
              predicates.foreach(predicatesElement => {
                val predicateNode = predicatesElement;
                objects.foreach(objectsElement => {
                  unionGraphs.foreach(unionGraph => {
                    val tpS = subjectNode;
                    //val tpP = predicatesElement._1;
                    val tpO = objectsElement;
                    val tpG = unionGraph;
                    this.materializer.materializeQuad(tpS, predicateNode, tpO, tpG);
                  })
                });

                refObjects.foreach(refObjectsElement => {
                  this.materializer.materializeQuad(subjectNode, predicateNode, refObjectsElement, unionGraph)
                });

              });
            })
          }

        });
        i = i+1;
      } catch {
        case e:Exception => {
          noOfErrors = noOfErrors + 1;
          val errorMessage = e.getMessage;
          e.printStackTrace();
          logger.error("error while translating data: " + errorMessage);
        }
      }
    }

    if(noOfErrors > 0) {
      logger.debug("Error when generating " + noOfErrors + " triples, check log file for details!");
    }

    logger.info(i + " instances retrieved.");
    //logger.debug(setSubjects.size + " unique instances (URI) retrieved.");
    //logger.debug(setBetaSub.size + " unique instances (DB Column) retrieved.");
    rows.close();

  }


  def visit(triplesMap:R2RMLTriplesMap) : Object = {
    //		String sqlQuery = triplesMap.accept(
    //				new R2RMLElementUnfoldVisitor()).toString();
    this.translateData(triplesMap);
    null;
  }

  override def generateRDFTriples(cm:MorphBaseClassMapping , iQuery:IQuery ) = {
    val triplesMap = cm.asInstanceOf[R2RMLTriplesMap];
    val logicalTable = triplesMap.getLogicalTable().asInstanceOf[R2RMLLogicalTable];
    val sm = triplesMap.subjectMap;
    val poms = triplesMap.predicateObjectMaps;
    this.generateRDFTriples(logicalTable, sm, poms, iQuery);
  }

  override def generateSubjects(cm:MorphBaseClassMapping, iQuery:IQuery) = {
    val triplesMap = cm.asInstanceOf[R2RMLTriplesMap];
    val logicalTable = triplesMap.getLogicalTable().asInstanceOf[R2RMLLogicalTable];
    val sm = triplesMap.subjectMap;
    this.generateRDFTriples(logicalTable, sm, Nil, iQuery);
    //conn.close();
  }



  def translateDateTime(value:String) = {
    value.toString().trim().replaceAll(" ", "T");
  }

  def translateDate(value:String) = {
    //val dfInput = new SimpleDateFormat("dd-MMM-yyy", Locale.ENGLISH);
    val result = dfInput.parse(value);
    val dfOutput = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
    val result2 = dfOutput.format(result);
    result2.toString();
  }

  def translateBoolean(value:String) = {
    if(value.equalsIgnoreCase("T")  || value.equalsIgnoreCase("True") || value.equalsIgnoreCase("1")) {
      "true";
    } else if(value.equalsIgnoreCase("F") || value.equalsIgnoreCase("False") || value.equalsIgnoreCase("0")) {
      "false";
    } else {
      "false";
    }
  }

  def createLiteral(value:Object, datatype:Option[String]
                    , language:Option[String]) : Literal = {
    try {
      val encodedValueAux = GeneralUtility.encodeLiteral(value.toString());
      //			val encodedValue = if(this.properties != null) {
      //				if(this.properties.literalRemoveStrangeChars) {
      //				  GeneralUtility.removeStrangeChars(encodedValueAux);
      //				} else { encodedValueAux }
      //			} else { encodedValueAux }
      val encodedValue = encodedValueAux;

      val valueWithDataType = if(datatype.isDefined && datatype.get != null) {
        val datatypeGet = datatype.get;


        //				datatypeGet match {
        //					case xsdDateTimeURI => {
        //					  this.translateDateTime(encodedValue);
        //					}
        //					case xsdBooleanURI => {
        //					  this.translateBoolean(encodedValue);
        //				  	}
        //					case _ => {
        //					  encodedValue
        //					}
        //				  }

        if(datatypeGet.equals(xsdDateTimeURI)) {
          this.translateDateTime(encodedValue);
        }
        else if (datatypeGet.equals(xsdBooleanURI)) {
          this.translateBoolean(encodedValue);
        }
        else if(datatypeGet.equals(xsdDurationURI)) {
          encodedValue;
        }
        else if(datatypeGet.equals(xsdDateURI)) {
          this.translateDate(encodedValue)
        }
        else {
          encodedValue
        }
      } else { encodedValue }

      val result:Literal = if(language.isDefined) {
        this.materializer.model.createLiteral(valueWithDataType, language.get);
      } else {
        if(datatype.isDefined) {
          this.materializer.model.createTypedLiteral(valueWithDataType, datatype.get);
        } else {
          this.materializer.model.createLiteral(valueWithDataType);
        }
      }

      //			val result:Literal = if(datatype.isDefined) {
      //			  this.materializer.model.createTypedLiteral(encodedValue, datatype.get);
      //			} else {
      //				if(language.isDefined) {
      //				  this.materializer.model.createLiteral(encodedValue, language.get);
      //				} else {
      //				  this.materializer.model.createLiteral(encodedValue);
      //				}
      //			}
      result
    } catch {
      case e:Exception => {
        logger.warn("Error translating value : " + value);
        throw e
      }
    }
  }

  //	def translateData2(termMap:R2RMLTermMap, originalValue:Object
  //	    , mapXMLDatatype : Map[String, String]) = {
  //		val translatedValue:String = termMap.inferTermType match {
  //		  case Constants.R2RML_IRI_URI => {
  //			 this.translateIRI(originalValue.toString());
  //		  }
  //		  case Constants.R2RML_LITERAL_URI => {
  //			  this.translateLiteral(termMap, originalValue, mapXMLDatatype);
  //		  }
  //		  case Constants.R2RML_BLANKNODE_URI => {
  //		    val resultBlankNode = GeneralUtility.createBlankNode(originalValue.toString());
  //		    resultBlankNode
  //		  }
  //		  case _ => {
  //			  originalValue.toString()
  //		  }
  //		}
  //		translatedValue
  //	}

  def translateBlankNode(value:Object) = {

  }




  def generateNode(rs:ResultSet, termMap:R2RMLTermMap, mapXSDDatatype:Map[String, String]
                   , logicalTableAlias:String
                   //) : (RDFNode, List[Object]) = {
                  ) : Node = {
    val result = termMap.termMapType match {
      case Constants.MorphTermMapType.ConstantTermMap => {
        this.nodeGenerator.generateNodeFromConstantMap(termMap);
      }
      case Constants.MorphTermMapType.ColumnTermMap => {
        val columnTermMapValue = if(logicalTableAlias != null && !logicalTableAlias.equals("")) {
          val termMapColumnValueSplit = termMap.columnName.split("\\.");
          //val columnName = termMapColumnValueSplit(termMapColumnValueSplit.length - 1).replaceAll("\"", dbEnclosedCharacter);
          //val columnName = termMapColumnValueSplit(termMapColumnValueSplit.length - 1).replaceAll(dbEnclosedCharacter, "");
          val columnName = termMapColumnValueSplit(termMapColumnValueSplit.length - 1).replaceAllLiterally("\\\"", "");

          logicalTableAlias + "_" + columnName;
        }
        else { termMap.columnName }

        this.nodeGenerator.generateNodeFromColumnMap(termMap, rs, mapXSDDatatype, columnTermMapValue);
      }
      case Constants.MorphTermMapType.TemplateTermMap => {
        //this.nodeGenerator.generateNodeFromTemplateMap(termMap, rs, logicalTableAlias, null, null);

        val dbType = this.properties.databaseType;
        val dbEnclosedCharacter = Constants.getEnclosedCharacter(dbType);
        val termMapTemplateString = termMap.templateString.replaceAllLiterally("\\\"", dbEnclosedCharacter);
        val attributes = RegexUtility.getTemplateColumns(termMapTemplateString, true);
        val mapTemplateColumns:Map[String, String] = attributes.map(attribute => {
          val databaseColumn = if(logicalTableAlias != null) {
            val attributeSplit = attribute.split("\\.");
            if(attributeSplit.length >= 1) {
              val columnName = attributeSplit(attributeSplit.length - 1).replaceAll("\"", dbEnclosedCharacter);
              logicalTableAlias + "_" + columnName;
            }
            else { logicalTableAlias + "_" + attribute; }
          }  else { attribute; }
          (attribute -> databaseColumn)
        }).toMap;

        this.nodeGenerator.generateNodeFromTemplateMap(termMap, rs, mapTemplateColumns);
      }
    }

    result
  }


}