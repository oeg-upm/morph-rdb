package es.upm.fi.dia.oeg.morph.base.materializer

//import com.hp.hpl.jena.rdf.model.Model
import org.apache.jena.graph.Node
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.impl.PropertyImpl
import org.apache.jena.riot.Lang
import org.apache.jena.sparql.core.Quad;
//import org.apache.log4j.Logger
import java.io.Writer
import java.io.OutputStreamWriter
import java.io.FileOutputStream
//import com.hp.hpl.jena.rdf.model.RDFNode
import org.apache.jena.rdf.model.RDFNode;
import es.upm.fi.dia.oeg.morph.base.GeneralUtility
import java.io.OutputStream
import java.io.PrintWriter
import java.io.BufferedOutputStream
import java.io.BufferedWriter
//import com.hp.hpl.jena.rdf.model.Property
import org.apache.jena.rdf.model.Property;
import org.slf4j.LoggerFactory

import org.apache.jena.riot.system.StreamOps
import org.apache.jena.riot.system.StreamRDF
import org.apache.jena.riot.system.StreamRDFWriter
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class NTripleMaterializer(model:Model,ntWriter:Writer, ntOutputStream: OutputStream)
  extends MorphBaseMaterializer(model,ntWriter, ntOutputStream) {
  //THIS IS IMPORTANT, SCALA PASSES PARAMETER BY VALUE!
  this.writer = ntWriter;
  this.outputStream = ntOutputStream


  override 	val logger = LoggerFactory.getLogger(this.getClass());


  def write(triple:String ) = {
    this.writer.write(triple)
    this.writer.flush();
  }

  def streamQuad(quad:Quad) = {
    val graphNode = quad.getGraph;
    if(graphNode == null) {
      val triple = quad.asTriple()
      val streamRDF:StreamRDF = StreamRDFWriter.getWriterStream(outputStream, Lang.NTRIPLES)
      streamRDF.start()
      val tripleIterator = List(triple).toIterator.asJava;
      StreamOps.sendTriplesToStream(tripleIterator, streamRDF)
      streamRDF.finish()
    } else {
      val quadIterator = List(quad).toIterator.asJava;
      val streamRDF:StreamRDF = StreamRDFWriter.getWriterStream(outputStream, Lang.NQUADS)
      streamRDF.start()
      StreamOps.sendQuadsToStream(quadIterator, streamRDF)
      streamRDF.finish()
    }

  }

  override def materialize() {
    //nothing to do, the triples were added during the data translation process
    if(this.writer != null) { this.writer.flush(); }
    if(this.outputStream != null) {
      this.outputStream.flush()
      this.outputStream.close()
    }

  }

  override def materializeQuad(subjectNode:Node, predicateNode:Node, objectNode:Node, graphNode:Node) {
    if(subjectNode != null && predicateNode != null && objectNode != null) {
      try {
        /*
				val subjectString = GeneralUtility.nodeToString(subject);
				val predicateString = GeneralUtility.nodeToString(predicate);
				val objectString = GeneralUtility.nodeToString(obj);
				val graphString = GeneralUtility.nodeToString(graph);
				val tripleString = GeneralUtility.createQuad(subjectString, predicateString, objectString, graphString);
				this.write(tripleString);
				*/

        /*
        val graphNode = if(graph != null) { graph.asNode() } else { null }
        val subjectNode = if(subject != null) { subject.asNode() } else { null }
        val objectNode = if(obj != null) { obj.asNode() } else { null }
        */
        //val predicateNode = if(predicate != null) { predicate.asNode() } else { null }
        val quad = Quad.create(graphNode, subjectNode, predicateNode, objectNode)
        this.streamQuad(quad)

      } catch {
        case e:Exception => {
          //e.printStackTrace();
          val errorMessage = "unable to serialize triple, subjectURI=" + subjectNode + ", error message = " + e.getMessage();
          logger.debug(errorMessage);
          //noOfErrors = noOfErrors + 1;
        }
      }
    } else {
      if(subjectNode == null) {
        val errorMessage = "unable to serialize triple, subject is null!";
        logger.debug(errorMessage);
      }

      if(predicateNode == null) {
        val errorMessage = "unable to serialize triple, predicate is null!";
        logger.debug(errorMessage);
      }

      if(objectNode == null) {
        val errorMessage = "unable to serialize triple, object is null!";
        //logger.debug(errorMessage);
      }


    }
  }

  //	override def postMaterialize() = {
  ////		if(this.writer != null) {
  ////			this.writer.flush();
  ////			this.writer.close();
  ////		}
  //	}
}