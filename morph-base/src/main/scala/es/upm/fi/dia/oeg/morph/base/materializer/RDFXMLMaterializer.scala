package es.upm.fi.dia.oeg.morph.base.materializer

//import org.apache.log4j.Logger
//import com.hp.hpl.jena.rdf.model.Resource
import java.io.FileOutputStream

import org.apache.jena.graph.Node
import org.apache.jena.rdf.model.Resource
import org.apache.jena.riot.RDFFormat
//import com.hp.hpl.jena.rdf.model.RDFNode
import org.apache.jena.rdf.model.RDFNode;
//import com.hp.hpl.jena.rdf.model.Model
import org.apache.jena.rdf.model.Model;
import java.io.OutputStream
import java.io.Writer
//import com.hp.hpl.jena.rdf.model.Property
import org.apache.jena.rdf.model.Property;
import org.slf4j.LoggerFactory

class RDFXMLMaterializer(model:Model, rdfxmlWriter:Writer, rdfxmlOutputStream:OutputStream)
  extends MorphBaseMaterializer(model, rdfxmlWriter, rdfxmlOutputStream) {
  //THIS IS IMPORTANT, SCALA PASSES PARAMETER BY VALUE!
  this.writer = rdfxmlWriter;
  this.outputStream = rdfxmlOutputStream

  override 	val logger = LoggerFactory.getLogger(this.getClass());

  //var outputFileName:String=null;

  //
  //	@Override
  //	public
  //	void materializeRDFTypeTriple(String subjectURI, String conceptName,
  //			boolean isBlankNodeSubject, String graph) {
  //		this.currentSubject = (Resource) this.createSubject(isBlankNodeSubject, subjectURI);
  //		Resource object = model.getResource(conceptName);
  //		Statement statement = model.createStatement(this.currentSubject, RDF.type, object);
  //		model.add(statement);
  //	}
  //
  //
  //	@Override
  //	public void materializeDataPropertyTriple(String predicateName,
  //			Object propVal, String datatype,
  //			String lang, String graph) {
  //		Literal literal;
  //
  //		if(datatype == null) {
  //			String propValString = propVal.toString();
  //
  //			if(lang == null) {
  //				literal =  model.createTypedLiteral(propValString);
  //			} else {
  //				literal =  model.createLiteral(propValString, lang);
  //			}
  //		} else {
  //			literal =  model.createTypedLiteral(propVal, datatype);
  //		}
  //
  //		currentSubject.addProperty(model.createProperty(predicateName), literal);
  //
  //	}
  //
  //	@Override
  //	public void materializeObjectPropertyTriple(String predicateName,
  //			String rangeURI, boolean isBlankNodeObject, String graph) {
  //		Resource object;
  //		Property property = model.createProperty(predicateName);
  //
  //		if(isBlankNodeObject) {
  //			AnonId anonId = new AnonId(rangeURI);
  //			object = model.createResource(anonId);
  //		} else {
  //			object = model.createResource(rangeURI);
  //		}
  //
  //		Statement rdfstmtInstance = model.createStatement(currentSubject,property, object);
  //		model.add(rdfstmtInstance);
  //	}
  //
  //
  //	@Override
  //	public Resource createSubject(boolean isBlankNode, String subjectURI) {
  //		if(isBlankNode) {
  //			AnonId anonId = new AnonId(subjectURI);
  //			this.currentSubject = model.createResource(anonId);
  //		} else {
  //			this.currentSubject = model.createResource(subjectURI);
  //		}
  //		return this.currentSubject;
  //	}


  override def materialize() = {
    try {
      if(model != null) {
        logger.debug("Size of model = " + model.size());
        //logger.info("Writing model to " + this.outputStream.  + " ......");
        val startWritingModel = System.currentTimeMillis();
        //val fos = new FileOutputStream(outputFileName);
        model.write(this.writer, "RDF/XML");
        this.writer.flush();
        //				this.outputStream.close();
        val endWritingModel = System.currentTimeMillis();
        val durationWritingModel = (endWritingModel-startWritingModel) / 1000;
        logger.info("Writing model time was "+(durationWritingModel)+" s.");
      }
    } catch {
      case e:Exception => {
        logger.error("Error materializing: " + e.getMessage);
        throw e;
      }
    }
  }



  override def materializeQuad(subjectNode:Node, predicateNode:Node, objectNode:Node, graphNode:Node ) {
    //val subjectRDFNode = this.model.asRDFNode(subjectNode);
    //val resource = this.model.createResource(subjectNode.getURI)
    val property = this.model.createProperty(predicateNode.getNameSpace, predicateNode.getLocalName)
    val objectRDFNode = if(objectNode != null) {
      this.model.asRDFNode(objectNode);
    } else { null }

    /*
    if(!subjectRDFNode.isResource()) {
      logger.error("Subject: " + subjectRDFNode + " is not a resource!");
    }
    */

    /*
    if(!predicate.isProperty) {
      logger.error("Predicate: " + predicate + " is not a property!");
    }
    */


    //val subjectResource = subject.asResource();
    //val subjectResource = subjectRDFNode.asResource();
    val subjectResource = this.model.createResource(subjectNode.getURI);


    if(objectRDFNode != null) {
      try {
        //subjectResource.addProperty(predicate, obj);

        this.model.add(subjectResource, property, objectRDFNode)
      } catch {
        case e:Exception => {
          logger.error("Error adding property: " + property + ", error message = " + e.getMessage() );
        }
      }
    }






  }

  //	override def postMaterialize() {
  //		// TODO Auto-generated method stub
  //	}
}