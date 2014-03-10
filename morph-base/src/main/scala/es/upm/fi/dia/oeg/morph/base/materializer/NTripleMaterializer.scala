package es.upm.fi.dia.oeg.morph.base.materializer

import com.hp.hpl.jena.rdf.model.Model
import org.apache.log4j.Logger
import java.io.Writer
import java.io.OutputStreamWriter
import java.io.FileOutputStream
import com.hp.hpl.jena.rdf.model.RDFNode
import es.upm.fi.dia.oeg.morph.base.GeneralUtility

class NTripleMaterializer(model:Model) extends MorphBaseMaterializer(model:Model) {
	override val logger = Logger.getLogger(this.getClass().getName());
//	var currentSubject:String =null;
	var writer:Writer  = null;
	var outputFileName:String = null;
	
	def write(triple:String ) = {
		if(this.writer == null) {
			this.writer = new OutputStreamWriter (new FileOutputStream (this.outputFileName), "UTF-8");
		}
		writer.append(triple);
	}

//	override def materializeRDFTypeTriple(String subjectURI, String conceptName,
//			boolean isBlankNodeSubject, String graph) {
//		String triple = null;
//		try {
//			this.currentSubject = this.createSubject(isBlankNodeSubject, subjectURI);
//			//this.currentSubject = this.currentSubject.replaceAll("\n","").replaceAll("\r", "");
//
//			triple = 
//					GeneralUtility.createQuad(
//							this.currentSubject
//							, GeneralUtility.createURIref(RDF.type.toString())
//							, GeneralUtility.createURIref(conceptName)
//							, GeneralUtility.createURIref(graph)
//							); 
//
//			//writer.append(triple);
//			this.write(triple);
//		} catch(Exception e) {
//			logger.error("unable to serialize triple, subjectURI=" + subjectURI);
//		}
//	}
//
//	@Override
//	public void materializeDataPropertyTriple(String predicateName,
//			Object propVal, String datatype,
//			String lang, String graph) {
//		String triplePredicate = GeneralUtility.createURIref(predicateName);
//
//		String propValString = propVal.toString();
//
//		String literalString = null;
//		if(datatype == null) {
//			if(lang == null) {
//				literalString = GeneralUtility.createLiteral(propValString);
//			} else {
//				literalString = GeneralUtility.createLanguageLiteral(propValString, lang);
//			}
//		} else {
//			literalString = GeneralUtility.createDataTypeLiteral(propValString, datatype);
//		}
//
//		String tripleString = null; 
//		try {
//			if(this.currentSubject != null) {
//				tripleString = GeneralUtility.createQuad(this.currentSubject, triplePredicate, literalString, GeneralUtility.createURIref(graph));
//				//writer.append(tripleString);
//				this.write(tripleString);
//			}
//		} catch(Exception e) {
//			logger.error("unable to serialize triple : " + tripleString + " because " + e.getMessage());
//		}
//	}
//
//
//
//	@Override
//	public void materializeObjectPropertyTriple(String predicateName,
//			String rangeURI, boolean isBlankNodeObject, String graph) {
//
//		String objectString;
//		if(isBlankNodeObject) {
//			objectString = GeneralUtility.createBlankNode(rangeURI);
//		} else {
//			objectString = GeneralUtility.createURIref(rangeURI);
//		}
//
//		String triple = null;
//		try {
//			if(this.currentSubject != null) {
//				triple = GeneralUtility.createQuad( this.currentSubject
//						, GeneralUtility.createURIref(predicateName)
//						, objectString, GeneralUtility.createURIref(graph)); 
//				this.write(triple);				
//			}
//		} catch(Exception e) {
//			logger.error("unable to serialize triple : " + triple);
//		}
//
//	}


//	@Override
//	public String createSubject(boolean isBlankNode, String subjectURI) {
//		if(isBlankNode) {
//			AnonId anonId = new AnonId(subjectURI);
//			model.createResource(anonId);		
//		} else {
//			model.createResource(subjectURI);
//		}
//
//		if(isBlankNode) {
//			this.currentSubject = GeneralUtility.createBlankNode(subjectURI);
//		} else {
//			this.currentSubject = GeneralUtility.createURIref(subjectURI);
//			
//			try {
//				GeneralUtility.substituteEntitiesInElementContent(this.currentSubject);
//			} catch(Exception e) {
//				logger.warn("Not well formed address : " + this.currentSubject);
//			}
//		}
//		return this.currentSubject;
//	}


	override def materialize() {
		//nothing to do, the triples were added during the data translation process
		if(this.writer != null) {
			this.writer.flush();
			this.writer.close();
		}
	}
	
//	@Override
//	public void materializeQuad(String subject, String predicate, String object, String graph) {
//		String quad = GeneralUtility.createQuad(subject, predicate, object, graph);
//		try {
//			this.write(quad);
//		} catch(Exception e) {
//			logger.error("unable to serialize triple, subjectURI=" + quad);
//		}
//	}

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

	override def postMaterialize() = {
//		if(this.writer != null) {
//			this.writer.flush();
//			this.writer.close();
//		}
	}
}