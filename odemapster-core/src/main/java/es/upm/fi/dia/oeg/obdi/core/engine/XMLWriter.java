package es.upm.fi.dia.oeg.obdi.core.engine;

import java.util.Collection;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.hp.hpl.jena.query.Query;

import es.upm.fi.dia.oeg.obdi.core.XMLUtility;

public class XMLWriter extends AbstractQueryResultWriter {

	private static Logger logger = Logger.getLogger(XMLWriter.class);
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
				String translatedColumnValue = queryTranslator.translateResultSet(varName, rs);
				bindingElement.setTextContent(translatedColumnValue);
				resultElement.appendChild(bindingElement);
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
