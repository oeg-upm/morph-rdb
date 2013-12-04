package es.upm.fi.dia.oeg.obdi.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.xerces.parsers.DOMParser;
import org.apache.xml.serialize.OutputFormat;
import org.apache.xml.serialize.XMLSerializer;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

public class XMLUtility {
	private static Logger logger = Logger.getLogger(XMLUtility.class);
	
	public static void main(String args[]) throws Exception, ParserConfigurationException, TransformerException
	{
		PropertyConfigurator.configure("log4j.properties");
		logger.debug("logger initialized");
	}
	
	public static Document removeFromRootElement(Document document, String nodeName, String attributeName, String attributeValue) {
		try
		{
			Element rootElement = document.getDocumentElement();
			
			NodeList nl = document.getElementsByTagName(nodeName);
			Vector<Node> deletedNodes = new Vector<Node>();
			
			for(int i=0; i<nl.getLength(); i++) {
				Node node = nl.item(i);
				String noteAttributeValue = node.getAttributes().getNamedItem(attributeName).getNodeValue();

				if(noteAttributeValue.equals(attributeValue))
				{
					deletedNodes.add(node);
				}
				
				
			}
			
			for(Node deletedNode : deletedNodes) {
				rootElement.removeChild(deletedNode);
			}
			
			
		} catch(Exception e) {
			e.printStackTrace();
		}

		return document;
	}

	public static Document createNewXMLDocument() throws ParserConfigurationException 
	{
		DocumentBuilderFactory documentBuilderFactory = 
			DocumentBuilderFactory.newInstance();
		DocumentBuilder documentBuilder = 
			documentBuilderFactory.newDocumentBuilder();
		Document document = documentBuilder.newDocument();

		return document;
	}

	public static Document loadXMLFile(String fileAbsolutePath) throws ParserConfigurationException, SAXException, IOException
	{
		try {
			System.currentTimeMillis();
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder docBuilder = factory.newDocumentBuilder();
			Document xmlDocument = docBuilder.parse(fileAbsolutePath);
			System.currentTimeMillis();
			return xmlDocument;			
		} catch(FileNotFoundException fnfe) {
			logger.error("File " + fileAbsolutePath + " can not be found!");
			throw fnfe;
		} catch(SAXParseException saxpe) {
			logger.error("Error while parsing the xml file " + fileAbsolutePath);
			throw saxpe;			
		}
		

	}

	public static Document convertToXMLDocument(String xmlString) throws ParserConfigurationException, SAXException, IOException
	{
		System.currentTimeMillis();
		
		DOMParser parser = new DOMParser();
		parser.parse(new InputSource(new java.io.StringReader(xmlString)));
		Document xmlDocument = parser.getDocument();
		System.currentTimeMillis();
		return xmlDocument;
	}
	
	public static Element getRootElement(String fileLocation) throws ParserConfigurationException, SAXException, IOException {
		return XMLUtility.loadXMLFile(fileLocation).getDocumentElement();
	}
	
	public static void appendChilds(Element element, Vector<Element> childElements) {
		if(element != null && childElements != null) {
			for(Element childElement : childElements) {
				element.appendChild(childElement);
			}			
		}
	}
	
	public static Vector<Element> toVectorElements(Element element) {
		Vector<Element> elements = new Vector<Element>();
		elements.add(element);
		return elements;
	}

	public static Element getFirstElement(Element element) {
		Element result = null;
		NodeList nl = element.getChildNodes();
		for(int i=0; i<nl.getLength(); i++) {
			if(nl.item(i) instanceof Element) {
				result = (Element) nl.item(i);
			}
		}
		return result;
	}

	public static Element getFirstChildElementByTagName(Element element, String name) {
		boolean found = false;
		Element result = null;
		NodeList nl = element.getChildNodes();
		for(int i=0; i<nl.getLength() && !found; i++) {
			if(nl.item(i) instanceof Element) {
				if(name.equals(nl.item(i).getNodeName())) {
					result = (Element) nl.item(i);
					found = true;
				}				
			}

		}
		return result;
	}
	
	public static List<Element> getChildElements(Element element) {
		List<Element> result = new ArrayList<Element>();
		NodeList childNodes = element.getChildNodes();
		for(int i=0; i<childNodes.getLength(); i++) {
			if(childNodes.item(i) instanceof Element) {
				result.add((Element) childNodes.item(i));
			}
		}
		return result;
	}

	public static List<Element> getChildElementsByTagName(Element element, String name) {
		List<Element> result = new ArrayList<Element>();
		
		NodeList childNodes = element.getChildNodes();
		for(int i=0; i<childNodes.getLength(); i++) {
			Node node = childNodes.item(i); 
			if(node instanceof Element) {
				String nodeName = node.getNodeName();
				if(nodeName != null) {
					if(nodeName.equals(name)) {
						result.add((Element) childNodes.item(i));
					}
				}
				
			}
		}
		
		if(result.size() == 0) {
			result = null;
		}
		return result;
	}

	public static String printXMLDocument(
			Document document, boolean indenting, boolean omitXMLDeclaration) 
	throws TransformerException, IOException
	{
		StringWriter writer = new StringWriter();
		OutputFormat format = new OutputFormat(document);
		format.setIndenting(indenting);
		format.setOmitXMLDeclaration(omitXMLDeclaration);

		XMLSerializer serializer = new XMLSerializer(writer, format);
		serializer.serialize(document);
		String inputString = writer.toString();
		return inputString;
	}
	/**
	 * This method uses Xerces specific classes
	 * prints the XML document to file.
     */
	public static void saveXMLDocument(Document document, String filename){

		try
		{
			//print
			OutputFormat format = new OutputFormat(document);
			format.setIndenting(true);

			//to generate output to console use this serializer
			//XMLSerializer serializer = new XMLSerializer(System.out, format);


			//to generate a file output use fileoutputstream instead of system.out
			XMLSerializer serializer = new XMLSerializer(new FileOutputStream(new File(filename)), format);
			
			serializer.serialize(document);

		} catch(IOException ie) {
		    ie.printStackTrace();
		}
	}

	/**
	 * @param docBuilder
	 *          the parser
	 * @param parent
	 *          node to add fragment to
	 * @param fragment
	 *          a well formed XML fragment
	 * @throws ParserConfigurationException 
	 */
	public static void appendXmlFragment(Node parent, String fragment) throws IOException, SAXException, ParserConfigurationException {
		DocumentBuilderFactory dbf =
			DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();

		Document doc = parent.getOwnerDocument();
		Node fragmentNode = db.parse(
				new InputSource(new StringReader(fragment)))
				.getDocumentElement();
		fragmentNode = doc.importNode(fragmentNode, true);
		parent.appendChild(fragmentNode);
	}
	
	public static String toOpenTag(String tagName) {
		return "<" + tagName + ">";
	}
	
	public static String toCloseTag(String tagName) {
		return "</" + tagName + ">";
	}
}

