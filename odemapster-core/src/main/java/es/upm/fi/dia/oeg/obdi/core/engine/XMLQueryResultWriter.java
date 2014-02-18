package es.upm.fi.dia.oeg.obdi.core.engine;

import java.util.Collection;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.hp.hpl.jena.query.Query;

import es.upm.fi.dia.oeg.morph.base.Constants;
import es.upm.fi.dia.oeg.morph.base.TermMapResult;
import es.upm.fi.dia.oeg.morph.base.XMLUtility;

public class XMLQueryResultWriter extends AbstractQueryResultWriter {

	private static Logger logger = Logger.getLogger(XMLQueryResultWriter.class);
	private Document xmlDoc;
	private Element resultsElement;
	private String outputFileName;
	

	public void initialize() throws Exception {
		this.xmlDoc = XMLUtility.createNewXMLDocument();
	}

	public void preProcess() throws Exception {
		
		//create root element
		Element rootElement = xmlDoc.createElement("sparql");
		xmlDoc.appendChild(rootElement);

		//create head element
		Element headElement = xmlDoc.createElement("head");
		rootElement.appendChild(headElement);
		Query sparqlQuery = this.getQueryTranslator().getSPARQLQuery();
		Collection<String> varNames = sparqlQuery.getResultVars();
		for(String varName : varNames) {
			Element variableElement = xmlDoc.createElement("variable");
			variableElement.setAttribute("name", varName);
			headElement.appendChild(variableElement);
		}
		
		//create results element
		this.resultsElement = xmlDoc.createElement("results");
		rootElement.appendChild(resultsElement);
	}

	public void process() throws Exception {
		Query sparqlQuery = this.getQueryTranslator().getSPARQLQuery();
		Collection<String> varNames = sparqlQuery.getResultVars();
		
		int i=0;
		AbstractResultSet rs = super.getResultSet();
		while(rs.next()) {
			Element resultElement = xmlDoc.createElement("result");
			resultsElement.appendChild(resultElement);
			
			for(String varName : varNames) {
				Element bindingElement = xmlDoc.createElement("binding");
				bindingElement.setAttribute("name", varName);
				IQueryTranslator queryTranslator = super.getQueryTranslator();
				TermMapResult translatedColumnValue = queryTranslator.translateResultSet(varName, rs);
				String translatedValue = translatedColumnValue.translatedValue();
				resultElement.appendChild(bindingElement);

				String termType = translatedColumnValue.termType();
				if(termType != null) {
					String termTypeElementName = null;
					if(termType.equalsIgnoreCase(Constants.R2RML_IRI_URI())) {
						termTypeElementName = "uri";
					} else if(termType.equalsIgnoreCase(Constants.R2RML_LITERAL_URI())) {
						termTypeElementName = "literal";
					}
					Element termTypeElement = xmlDoc.createElement(termTypeElementName);
					bindingElement.appendChild(termTypeElement);
					termTypeElement.setTextContent(translatedValue);
				} else {
					bindingElement.setTextContent(translatedValue);	
				}
			}
			i++;
		}
		String status = i  + " instance(s) retrieved ";
		logger.info(status);
		
	}

	public void postProcess() throws Exception {
		logger.info("Writing result to " + outputFileName);
		XMLUtility.saveXMLDocument(xmlDoc, outputFileName);
	}


	@Override
	public Object getOutput() throws Exception {
		return this.xmlDoc;
	}

	@Override
	public void setOutput(Object output) throws Exception {
		this.outputFileName = (String) output;
	}

	
}
