package es.upm.fi.dia.oeg.morph.base.materializer

import org.apache.jena.graph.Node

import scala.collection.JavaConversions._
//import org.apache.log4j.Logger
//import com.hp.hpl.jena.rdf.model.Model
import org.apache.jena.rdf.model.Model;
//import com.hp.hpl.jena.rdf.model.RDFNode
import org.apache.jena.rdf.model.RDFNode;
import es.upm.fi.dia.oeg.morph.base.Constants
//import com.hp.hpl.jena.rdf.model.ModelFactory
import org.apache.jena.rdf.model.ModelFactory;
import java.io.File
//import com.hp.hpl.jena.tdb.TDBFactory
import org.apache.jena.tdb.TDBFactory
import java.io.OutputStream
import java.io.Writer
//import com.hp.hpl.jena.rdf.model.Property
import org.apache.jena.rdf.model.Property;
import org.slf4j.LoggerFactory

abstract class MorphBaseMaterializer(val model:Model, var writer:Writer, var outputStream: OutputStream) {
  //val logger = LogManager.getLogger(this.getClass);
  val logger = LoggerFactory.getLogger(this.getClass());

  //	var outputFileName:String = null;
  var rdfLanguage:String =null;
  //var noOfErrors:Long=0;

  //	def createSubject(isBlankNode:Boolean , subjectURI:String ):Object ;
  //	def materializeDataPropertyTriple(predicateName:String , objectValue:Object , datatype:String , lang:String , graph:String );
  //	def materializeObjectPropertyTriple(predicateName:String , rangeURI:String , isBlankNodeObject:Boolean , graph:String );
  //	def materializeRDFTypeTriple(subjectURI:String , conceptName:String , isBlankNodeSubject:boolean , graph:String );
  //	public abstract void materializeQuad(String subject, String predicate, String object, String graph);
  //def materializeQuad(subject:RDFNode , predicate:Property , obj:RDFNode , graph:RDFNode );
  def materializeQuad(subject:Node, predicate:Node, obj:Node, graph:Node);

  def materialize();

  def postMaterialize() = {
    if(this.writer != null) { this.writer.close() }
    if(this.outputStream != null) { this.outputStream.close() }
  }


  def setModelPrefixMap(prefixMap:Map[String, String] ) = {
    this.model.setNsPrefixes(prefixMap);
  }

}

object MorphBaseMaterializer {
  //val logger = LogManager.getLogger(this.getClass);
  val logger = LoggerFactory.getLogger(this.getClass());

  def createJenaModel(jenaMode:String ) : Model  = {
    val model = if(jenaMode == null) {
      MorphBaseMaterializer.createJenaMemoryModel();
    } else {
      if(jenaMode.equalsIgnoreCase(Constants.JENA_MODE_TYPE_TDB)) {
        //logger.debug("jena mode = tdb");
        //MorphBaseMaterializer.createJenaTDBModel();
        val errorMessage = "jena tdb not supported!";
        logger.error(errorMessage);
        throw new Exception(errorMessage);
      } else if (jenaMode.equalsIgnoreCase(Constants.JENA_MODE_TYPE_MEMORY)){
        //logger.debug("jena mode = memory");
        MorphBaseMaterializer.createJenaMemoryModel();
      } else {
        //logger.warn("invalid mode of jena type, memory mode will be used.");
        MorphBaseMaterializer.createJenaMemoryModel();
      }
    }

    model;
  }

  def createJenaMemoryModel() : Model  = { ModelFactory.createDefaultModel();	}

  /*
  def createJenaTDBModel() : Model  = {
    val jenaDatabaseName = System.currentTimeMillis() + "";
    val tdbDatabaseFolder = "tdb-database";
    val folder = new File(tdbDatabaseFolder);
    if(!folder.exists()) {
      folder.mkdir();
    }

    val tdbFileBase = tdbDatabaseFolder + "/" + jenaDatabaseName;
    logger.info("TDB filebase = " + tdbFileBase);
    return TDBFactory.createModel(tdbFileBase) ;

  }
  */

}