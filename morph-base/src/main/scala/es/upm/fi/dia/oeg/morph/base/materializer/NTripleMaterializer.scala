package es.upm.fi.dia.oeg.morph.base.materializer

import com.hp.hpl.jena.rdf.model.Model
import org.apache.log4j.Logger
import java.io.Writer
import java.io.OutputStreamWriter
import java.io.FileOutputStream
import com.hp.hpl.jena.rdf.model.RDFNode
import es.upm.fi.dia.oeg.morph.base.GeneralUtility
import java.io.OutputStream
import java.io.PrintWriter
import java.io.BufferedOutputStream
import java.io.BufferedWriter

class NTripleMaterializer(model:Model,ntOutputStream:Writer) 
extends MorphBaseMaterializer(model,ntOutputStream) {
	//THIS IS IMPORTANT, SCALA PASSES PARAMETER BY VALUE!
	this.outputStream = ntOutputStream;
	
	override val logger = Logger.getLogger(this.getClass().getName());

	def write(triple:String ) = {
		this.outputStream.write(triple)
		this.outputStream.flush();
	}


	override def materialize() {
		//nothing to do, the triples were added during the data translation process
		this.outputStream.flush();
//		this.outputStream.close();
	}
	
	override def materializeQuad(subject:RDFNode , predicate:RDFNode ,
			obj:RDFNode , graph:RDFNode ) {
		if(subject != null && predicate != null && obj!= null) {
			try {
				val subjectString = GeneralUtility.nodeToString(subject);
				val predicateString = GeneralUtility.nodeToString(predicate);
				val objectString = GeneralUtility.nodeToString(obj);
				val graphString = GeneralUtility.nodeToString(graph);
				
				val triple = GeneralUtility.createQuad(subjectString, predicateString, objectString, graphString);
				this.write(triple);
			} catch {
			  case e:Exception => {
			    e.printStackTrace()
			    logger.error("unable to serialize triple, subjectURI=" + subject);
			  }
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