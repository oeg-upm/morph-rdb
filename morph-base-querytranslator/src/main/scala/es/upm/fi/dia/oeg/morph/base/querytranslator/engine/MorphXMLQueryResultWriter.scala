package es.upm.fi.dia.oeg.morph.base.querytranslator.engine

import scala.collection.JavaConversions._
import org.w3c.dom.Document
import org.w3c.dom.Element
import org.apache.jena.query.Query
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.TermMapResult
import es.upm.fi.dia.oeg.morph.base.ValueTransformator
import es.upm.fi.dia.oeg.morph.base.XMLUtility
import es.upm.fi.dia.oeg.morph.base.engine.IQueryTranslator
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseQueryResultWriter
import java.io.OutputStream
import java.io.Writer
import org.apache.logging.log4j.LogManager


class MorphXMLQueryResultWriter(queryTranslator:IQueryTranslator, xmlOutputStream:Writer) 
extends MorphBaseQueryResultWriter(queryTranslator, xmlOutputStream) {
	this.outputStream = xmlOutputStream;
	
	val logger = LogManager.getLogger(this.getClass);
	
	if(queryTranslator == null) {
		throw new Exception("Query Translator is not set yet!");
    }
	
	val xmlDoc = XMLUtility.createNewXMLDocument();
	val resultsElement = xmlDoc.createElement("results");

	//var outputFileName:String = null;
	

	override def initialize() = { }

	def preProcess() = {
		
		//create root element
		val rootElement = xmlDoc.createElement("sparql");
		xmlDoc.appendChild(rootElement);

		//create head element
		val headElement = xmlDoc.createElement("head");
		rootElement.appendChild(headElement);
		val sparqlQuery = this.sparqlQuery;
		val varNames = sparqlQuery.getResultVars();
		for(varName <- varNames) {
			val variableElement = xmlDoc.createElement("variable");
			variableElement.setAttribute("name", varName);
			headElement.appendChild(variableElement);
		}
		
		//create results element
		rootElement.appendChild(resultsElement);
	}

	def process() = {
		val queryTranslator = this.queryTranslator;
		val sparqlQuery = this.sparqlQuery;
		val varNames = sparqlQuery.getResultVars();
		
		var i=0;
		val rs = this.resultSet;
		while(rs.next()) {
			val resultElement = xmlDoc.createElement("result");
			resultsElement.appendChild(resultElement);
			
			for(varName <- varNames) {
				val translatedColumnValue = queryTranslator.translateResultSet(varName, rs);
				if(translatedColumnValue != null) {
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
								Some("uri");
							  } else if(termType.equalsIgnoreCase(Constants.R2RML_LITERAL_URI)) {
								Some("literal");
							  } else {
								  null
							  }
							}

							if(termTypeElementName.isDefined) {
								val termTypeElement = xmlDoc.createElement(termTypeElementName.get);
								bindingElement.appendChild(termTypeElement);
								termTypeElement.setTextContent(lexicalValue);
							}

						} else {
							bindingElement.setTextContent(lexicalValue);	
						}				  
					}				  
				}

			}
			i = i+1;
		}
		val status = i  + " instance(s) retrieved ";
		logger.info(status);
		
	}

	def postProcess() = {
		//logger.info("Writing query result to " + outputStream);
		XMLUtility.saveXMLDocument(xmlDoc, outputStream);
	}


	override def getOutput() = {
		this.xmlDoc;
	}

}