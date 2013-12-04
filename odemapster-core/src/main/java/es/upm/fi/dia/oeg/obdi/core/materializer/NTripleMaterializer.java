package es.upm.fi.dia.oeg.obdi.core.materializer;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.rdf.model.AnonId;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.vocabulary.RDF;

import es.upm.fi.dia.oeg.obdi.core.ODEMapsterUtility;


public class NTripleMaterializer extends AbstractMaterializer {
	private String currentSubject;
	private static Logger logger = Logger.getLogger(NTripleMaterializer.class);
	private Writer writer;

	public NTripleMaterializer(String outputFileName, Model model) throws IOException {
		this.outputFileName = outputFileName;
		this.model = model;
		//this.writer = new OutputStreamWriter (new FileOutputStream (outputFileName), "UTF-8");
	}

	private void write(String triple) throws Exception {
		if(this.writer == null) {
			this.writer = new OutputStreamWriter (new FileOutputStream (outputFileName), "UTF-8");
		}
		writer.append(triple);
	}

	@Override
	public
	void materializeRDFTypeTriple(String subjectURI, String conceptName,
			boolean isBlankNodeSubject, String graph) {
		String triple = null;
		try {
			this.currentSubject = this.createSubject(isBlankNodeSubject, subjectURI);
			//this.currentSubject = this.currentSubject.replaceAll("\n","").replaceAll("\r", "");

			triple = 
					ODEMapsterUtility.createQuad(
							this.currentSubject
							, ODEMapsterUtility.createURIref(RDF.type.toString())
							, ODEMapsterUtility.createURIref(conceptName)
							, ODEMapsterUtility.createURIref(graph)
							); 

			//writer.append(triple);
			this.write(triple);
		} catch(Exception e) {
			logger.error("unable to serialize triple, subjectURI=" + subjectURI);
		}

	}

	@Override
	public void materializeDataPropertyTriple(String predicateName,
			Object propVal, String datatype,
			String lang, String graph) {
		String triplePredicate = ODEMapsterUtility.createURIref(predicateName);

		String propValString = propVal.toString();

		String literalString = null;
		if(datatype == null) {
			if(lang == null) {
				literalString = ODEMapsterUtility.createLiteral(propValString);
			} else {
				literalString = ODEMapsterUtility.createLanguageLiteral(propValString, lang);
			}
		} else {
			literalString = ODEMapsterUtility.createDataTypeLiteral(propValString, datatype);
		}

		String tripleString = null; 
		try {
			if(this.currentSubject != null) {
				tripleString = ODEMapsterUtility.createQuad(this.currentSubject, triplePredicate, literalString, ODEMapsterUtility.createURIref(graph));
				//writer.append(tripleString);
				this.write(tripleString);
			}
		} catch(Exception e) {
			logger.error("unable to serialize triple : " + tripleString + " because " + e.getMessage());
		}
	}



	@Override
	public void materializeObjectPropertyTriple(String predicateName,
			String rangeURI, boolean isBlankNodeObject, String graph) {

		String objectString;
		if(isBlankNodeObject) {
			objectString = ODEMapsterUtility.createBlankNode(rangeURI);
		} else {
			objectString = ODEMapsterUtility.createURIref(rangeURI);
		}

		String triple = null;
		try {
			if(this.currentSubject != null) {
				triple = ODEMapsterUtility.createQuad( this.currentSubject
						, ODEMapsterUtility.createURIref(predicateName)
						, objectString, ODEMapsterUtility.createURIref(graph)); 
				this.write(triple);				
			}
		} catch(Exception e) {
			logger.error("unable to serialize triple : " + triple);
		}

	}


	@Override
	public String createSubject(boolean isBlankNode, String subjectURI) {
		if(isBlankNode) {
			AnonId anonId = new AnonId(subjectURI);
			model.createResource(anonId);		
		} else {
			model.createResource(subjectURI);
		}

		if(isBlankNode) {
			this.currentSubject = ODEMapsterUtility.createBlankNode(subjectURI);
		} else {
			this.currentSubject = ODEMapsterUtility.createURIref(subjectURI);
			
			try {
				ODEMapsterUtility.substituteEntitiesInElementContent(this.currentSubject);
			} catch(Exception e) {
				logger.warn("Not well formed address : " + this.currentSubject);
			}
		}
		return this.currentSubject;
	}


	@Override
	public void materialize() throws IOException {
		//nothing to do, the triples were added during the data translation process
		if(this.writer != null) {
			//this.writer.flush();
			this.writer.close();
		}
	}
}
