package es.upm.fi.dia.oeg.obdi.core.materializer;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.rdf.model.AnonId;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.vocabulary.RDF;


public class RDFXMLMaterializer extends AbstractMaterializer {
	private static Logger logger = Logger.getLogger(RDFXMLMaterializer.class);
	
	private Resource currentSubject;
	
	public RDFXMLMaterializer(String outputFileName, Model model, String rdfLanguage) throws IOException {
		super.outputFileName = outputFileName;
		this.model = model;
		this.rdfLanguage = rdfLanguage;
	}

	@Override
	public
	void materializeRDFTypeTriple(String subjectURI, String conceptName,
			boolean isBlankNodeSubject, String graph) {
		this.currentSubject = (Resource) this.createSubject(isBlankNodeSubject, subjectURI);
		Resource object = model.getResource(conceptName);
		Statement statement = model.createStatement(this.currentSubject, RDF.type, object);
		model.add(statement);
	}


	@Override
	public void materializeDataPropertyTriple(String predicateName,
			Object propVal, String datatype,
			String lang, String graph) {
		Literal literal;
		
		if(datatype == null) {
			String propValString = propVal.toString();
			
			if(lang == null) {
				literal =  model.createTypedLiteral(propValString);
			} else {
				literal =  model.createLiteral(propValString, lang);
			}
		} else {
			literal =  model.createTypedLiteral(propVal, datatype);
		}
		
		currentSubject.addProperty(model.createProperty(predicateName), literal);
		
	}

	@Override
	public void materializeObjectPropertyTriple(String predicateName,
			String rangeURI, boolean isBlankNodeObject, String graph) {
		Resource object;
		Property property = model.createProperty(predicateName);

		if(isBlankNodeObject) {
			AnonId anonId = new AnonId(rangeURI);
			object = model.createResource(anonId);
		} else {
			object = model.createResource(rangeURI);
		}
		
		Statement rdfstmtInstance = model.createStatement(currentSubject,property, object);
		model.add(rdfstmtInstance);		
	}


	@Override
	public Resource createSubject(boolean isBlankNode, String subjectURI) {
		if(isBlankNode) {
			AnonId anonId = new AnonId(subjectURI);
			this.currentSubject = model.createResource(anonId);		
		} else {
			this.currentSubject = model.createResource(subjectURI);
		}
		return this.currentSubject;
	}


	@Override
	public void materialize() throws IOException {
		try {
			if(model != null) {
				logger.debug("Size of model = " + model.size());
				logger.info("Writing model to " + outputFileName + " ......");
				long startWritingModel = System.currentTimeMillis();
				FileOutputStream fos = new FileOutputStream(outputFileName);
				model.write(fos, this.rdfLanguage);
				fos.flush();fos.close();
				long endWritingModel = System.currentTimeMillis();
				long durationWritingModel = (endWritingModel-startWritingModel) / 1000;
				logger.info("Writing model time was "+(durationWritingModel)+" s.");				
			}
		} catch(FileNotFoundException fnfe) {
			logger.error("File " + outputFileName + " can not be found!");
			throw fnfe;			
		} 	
	}
}
