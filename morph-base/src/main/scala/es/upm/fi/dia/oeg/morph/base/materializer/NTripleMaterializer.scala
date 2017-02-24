package es.upm.fi.dia.oeg.morph.base.materializer

//import com.hp.hpl.jena.rdf.model.Model
import org.apache.jena.rdf.model.Model;
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
import org.apache.logging.log4j.LogManager

class NTripleMaterializer(model:Model,ntOutputStream:Writer) 
extends MorphBaseMaterializer(model,ntOutputStream) {
	//THIS IS IMPORTANT, SCALA PASSES PARAMETER BY VALUE!
	this.outputStream = ntOutputStream;
	
	override val logger = LogManager.getLogger(this.getClass);

	def write(triple:String ) = {
		this.outputStream.write(triple)
		this.outputStream.flush();
	}


	override def materialize() {
		//nothing to do, the triples were added during the data translation process
		this.outputStream.flush();
//		this.outputStream.close();
	}
	
	override def materializeQuad(subject:RDFNode , predicate:Property ,
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
			    //e.printStackTrace();
          val errorMessage = "unable to serialize triple, subjectURI=" + subject + ", error message = " + e.getMessage();
			    logger.debug(errorMessage);
          //noOfErrors = noOfErrors + 1;
			  }
			}
		} else {
		  if(subject == null) {
          val errorMessage = "unable to serialize triple, subject is null!";
          logger.debug(errorMessage);
		  } 
		  
		  if(predicate == null) {
          val errorMessage = "unable to serialize triple, predicate is null!";
          logger.debug(errorMessage);
		  }

		  if(obj == null) {
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