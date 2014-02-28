package es.upm.fi.dia.oeg.morph.base.querytranslator

import scala.collection.JavaConversions._
import es.upm.fi.dia.oeg.obdi.core.engine.AbstractQueryResultWriter
import org.apache.log4j.Logger
import org.w3c.dom.Document
import org.w3c.dom.Element
import com.hp.hpl.jena.query.Query
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.TermMapResult
import es.upm.fi.dia.oeg.morph.base.ValueTransformator
import es.upm.fi.dia.oeg.morph.base.XMLUtility


class MorphXMLQueryResultWriter extends AbstractQueryResultWriter {
	val logger = Logger.getLogger("MorphXMLQueryResultWriter");
	var xmlDoc:Document = null;
	var resultsElement:Element  = null;
	var outputFileName:String = null;
	

	def initialize() = {
		this.xmlDoc = XMLUtility.createNewXMLDocument();
	}

	def preProcess() = {
		
		//create root element
		val rootElement = xmlDoc.createElement("sparql");
		xmlDoc.appendChild(rootElement);

		//create head element
		val headElement = xmlDoc.createElement("head");
		rootElement.appendChild(headElement);
		val sparqlQuery = this.getQueryTranslator().getSPARQLQuery();
		val varNames = sparqlQuery.getResultVars();
		for(varName <- varNames) {
			val variableElement = xmlDoc.createElement("variable");
			variableElement.setAttribute("name", varName);
			headElement.appendChild(variableElement);
		}
		
		//create results element
		this.resultsElement = xmlDoc.createElement("results");
		rootElement.appendChild(resultsElement);
	}

	def process() = {
		val queryTranslator = super.getQueryTranslator();
		val sparqlQuery = queryTranslator.getSPARQLQuery();
		val varNames = sparqlQuery.getResultVars();
		
		var i=0;
		val rs = super.getResultSet();
		while(rs.next()) {
			val resultElement = xmlDoc.createElement("result");
			resultsElement.appendChild(resultElement);
			
			for(varName <- varNames) {
				val translatedColumnValue = queryTranslator.translateResultSet(varName, rs);
				val translatedDBValue = translatedColumnValue.translatedValue;
				val xsdDataType = translatedColumnValue.xsdDatatype;
				val lexicalValue = ValueTransformator.transformToLexical(
				    translatedDBValue, xsdDataType)
				if(lexicalValue != null) {
					val bindingElement = xmlDoc.createElement("binding");
					bindingElement.setAttribute("name", varName);
					resultElement.appendChild(bindingElement);
	
					val termType = translatedColumnValue.termType;
					if(termType != null) {
						val termTypeElementName = { 
						  if(termType.equalsIgnoreCase(Constants.R2RML_IRI_URI)) {
							"uri";
						  } else if(termType.equalsIgnoreCase(Constants.R2RML_LITERAL_URI)) {
							"literal";
						  } else {
							  null
						  }
						}
						
						val termTypeElement = xmlDoc.createElement(termTypeElementName);
						bindingElement.appendChild(termTypeElement);
						termTypeElement.setTextContent(lexicalValue);
					} else {
						bindingElement.setTextContent(lexicalValue);	
					}				  
				}
			}
			i = i+1;
		}
		val status = i  + " instance(s) retrieved ";
		logger.info(status);
		
	}

	def postProcess() = {
		logger.info("Writing result to " + outputFileName);
		XMLUtility.saveXMLDocument(xmlDoc, outputFileName);
	}


	override def getOutput() = {
		this.xmlDoc;
	}

	override def setOutput(output:Object) = {
		this.outputFileName = output.asInstanceOf[String];
	}

}