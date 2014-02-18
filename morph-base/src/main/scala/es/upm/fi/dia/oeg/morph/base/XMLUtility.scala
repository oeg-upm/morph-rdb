package es.upm.fi.dia.oeg.morph.base

import org.w3c.dom.Document
import javax.xml.parsers.DocumentBuilderFactory
import org.apache.xml.serialize.OutputFormat
import org.apache.xml.serialize.XMLSerializer
import java.io.FileOutputStream
import java.io.File

class XMLUtility {

}

object XMLUtility {
	def createNewXMLDocument() : Document = { 
		val documentBuilderFactory = DocumentBuilderFactory.newInstance();
		val documentBuilder = documentBuilderFactory.newDocumentBuilder();
		val document = documentBuilder.newDocument();
		document;
	}
	
	/**
	 * This method uses Xerces specific classes
	 * prints the XML document to file.
     */
	def saveXMLDocument(document:Document , filename:String ) = {
		try
		{
			//print
			val format = new OutputFormat(document);
			format.setIndenting(true);

			//to generate output to console use this serializer
			//XMLSerializer serializer = new XMLSerializer(System.out, format);

			//to generate a file output use fileoutputstream instead of system.out
			val serializer = new XMLSerializer(new FileOutputStream(new File(filename)), format);
			serializer.serialize(document);
		} catch {
		  case e:Exception => {
		    e.printStackTrace();
		  }
		}
	}	
}