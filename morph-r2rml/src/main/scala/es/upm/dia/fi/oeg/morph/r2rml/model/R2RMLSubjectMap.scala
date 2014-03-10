package es.upm.dia.fi.oeg.morph.r2rml.model

import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.Constants
import com.hp.hpl.jena.rdf.model.Resource
import java.util.HashSet
import com.hp.hpl.jena.rdf.model.RDFNode

class R2RMLSubjectMap(termMapType:Constants.MorphTermMapType.Value
    , termType:Option[String], datatype:Option[String], languageTag:Option[String]
    , val classURIs:Set[String], val graphMaps:Set[R2RMLGraphMap] ) 
extends R2RMLTermMap(termMapType, termType, datatype, languageTag) {
	override val logger = Logger.getLogger(this.getClass().getName());
	
	val inferredTermType = this.inferTermType;
	if(inferredTermType != null && inferredTermType.equals(Constants.R2RML_LITERAL_URI)) {
		throw new Exception("Literal is not permitted in the subject map!");
	}
		
}

object R2RMLSubjectMap {
	val logger = Logger.getLogger(this.getClass().getName());
	
	def apply(rdfNode:RDFNode) : R2RMLSubjectMap = {
		val coreProperties = R2RMLTermMap.extractCoreProperties(rdfNode);
		//coreProperties = (termMapType, termType, datatype, languageTag)
		val termMapType = coreProperties._1;
		val termType = coreProperties._2;
		val datatype = coreProperties._3;
		val languageTag = coreProperties._4;
		
		val classURIs:Set[String] = rdfNode match {
		  case resourceNode:Resource => {
			  val classStatements = resourceNode.listProperties(Constants.R2RML_CLASS_PROPERTY);
			  val classURIsAux : Set[String]= if(classStatements != null) {
				  classStatements.map(classStatement => {
				    classStatement.getObject().toString();}).toSet;
			  } else {
				  Set.empty;
			  }
			  classURIsAux
		  }
		  case _ => { Set.empty }
		}
		
		val graphMaps:Set[R2RMLGraphMap] = rdfNode match {
		  case resourceNode:Resource => { R2RMLGraphMap.extractGraphMaps(resourceNode); }
		  case _ => {Set.empty}
		}

		val sm = new R2RMLSubjectMap(termMapType, termType, datatype
		    , languageTag, classURIs, graphMaps);
		sm.rdfNode = rdfNode;


		sm.parse(rdfNode)
		sm
	}
	

	def extractSubjectMaps(resource:Resource) : Set[R2RMLSubjectMap]= {
	  val tms = R2RMLTermMap.extractTermMaps(resource, Constants.MorphPOS.sub);
	  val result = tms.map(tm => tm.asInstanceOf[R2RMLSubjectMap]);
	  result;
	}

}