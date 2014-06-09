package es.upm.fi.dia.oeg.morph.base

import scala.collection.JavaConversions._

import org.apache.log4j.Logger
import java.net.URL
import com.hp.hpl.jena.shared.CannotEncodeCharacterException
import java.util.regex.Pattern
import com.hp.hpl.jena.rdf.model.RDFNode

class GeneralUtility {

}

object GeneralUtility {
	val logger = Logger.getLogger("GeneralUtility");

	def encodeLiteral(originalLiteral:String) : String = {
		var result = originalLiteral;
		try {
			if(result != null) {
				result = result.replaceAll("\\\\", "/");
				result = result.replaceAll("\"", "%22");
				result = result.replaceAll("\\\\n", " ");
				result = result.replaceAll("\\\\r", " ");
				result = result.replaceAll("\\\\ ", " ");
				result = result.replaceAll("_{2,}+", "_");
				result = result.replaceAll("\n","");
				result = result.replaceAll("\r", "");
				result = result.replace("\\ ", "/");
			}
		} catch {
		  case e:Exception => {
		    logger.error("Error encoding literal for literal = " + originalLiteral + " because of " + e.getMessage());
		  }
		}

		result;
	}



	def encodeURI(originalURI:String, mapURIEncodingChars:Map[String,String]
	, uriTransformationOperations:List[String] )  : String = {
		val resultAux = originalURI.trim();
		var result = resultAux;
		
		try {
			if(mapURIEncodingChars != null) {
				mapURIEncodingChars.foreach{case(key,value) => {
				  result = result.replaceAll(key, value)}};
			}
			
			//DO THIS ON DATA LEVEL, NOT ON URI LEVEL
/*			if(uriTransformationOperations != null) {
				uriTransformationOperations.foreach{
				  case Constants.URI_TRANSFORM_TOLOWERCASE => {
					  result = result.toLowerCase();
				  }
				  case Constants.URI_TRANSFORM_TOUPPERCASE => {
					  result = result.toUpperCase();
				  }
				  case _ => {}
				}
			}*/
		} catch {
		  case e:Exception => {
			logger.error("Error encoding uri for uri = " + originalURI + " because of " + e.getMessage());
			resultAux
		  }
		}

		result;
	}
	
	//Creates a quad
	def createQuad(subject:String , predicate:String , obj:String , graph:String ) = {
		val graphString = if(graph != null) {
			" " + graph;
		} else {
		  ""
		}
		
		val result = subject + " " + predicate + " " + obj + graphString + " .\n";    
		result;
	}
	
	def isNetResource(resourceAddress:String ) : Boolean  = {
		val result = try {
			val url = new URL(resourceAddress);
			val conn = url.openConnection();
			conn.connect();
			true;			
		} catch {
		  case e:Exception => { false }
			
		}
		
		result;
	}
	
	//Create blank node from id
	def createBlankNode(id:String) : String  =	{
		val result = "_:" + id; 
		result;
	}

	//Create URIREF from URI
	def createURIref(uri:String ) : String =	{
		if(uri == null) {
			null;
		} else {
			val result = "<" + uri + ">";   
			result;			
		}
	}

	def nodeToString(rdfNode:RDFNode) : String = {
	  if(rdfNode == null) {
	    null
	  } else {
		if(rdfNode.isURIResource()) {
		  this.createURIref(rdfNode.asResource().getURI())
		} 
		else if(rdfNode.isAnon()) {
		  val id = rdfNode.asResource().getId();
		  GeneralUtility.createBlankNode(id.getLabelString());
		}
		else if(rdfNode.isLiteral()) {
			val literalNode = rdfNode.asLiteral();
			val datatype = literalNode.getDatatype();
			val lang = literalNode.getLanguage();
			val literalValue = literalNode.getValue();
			val literalValueString = literalValue.toString();
			
			val literalString = if(datatype == null) {
				if(lang == null || lang.equals("")) {
					GeneralUtility.createLiteral(literalValueString);
				} else {
					GeneralUtility.createLanguageLiteral(literalValueString, lang);
				}
			} else {
				GeneralUtility.createDataTypeLiteral(literalValueString, datatype.getURI());
			}
			
			literalString
		}
		else { rdfNode.toString()}	    
	  }

	}
	
	//Create typed literal
	def createDataTypeLiteral(pvalue:String , datatypeURI:String ) : String = {
		val value = GeneralUtility.encodeLiteral(pvalue);
		val result = "\"" + value + "\"^^" + "<" + datatypeURI + ">";   
		result;
	}
	
	//Create language tagged literal
	def createLanguageLiteral(pText:String , languageCode:String ) : String =	{
		val text = GeneralUtility.encodeLiteral(pText);
		val result = "\"" + text + "\"@" + languageCode;
		result;
	}
	
	//Create Literal
	def createLiteral(pValue:String ) : String =	{
		val value = GeneralUtility.encodeLiteral(pValue);
		val result = "\"" + value + "\"";
		result
	}
	
	def encodeUnsafeChars(originalValue:String ) : String = {
		var result = originalValue; 
		if(result != null) {
			//result = result.replaceAll("\\%", "%25");//put this first
			result = result.replaceAll("<", "%3C");
			result = result.replaceAll(">", "%3E");
//			result = result.replaceAll("#", "%23");

			result = result.replaceAll("\\{", "%7B");
			result = result.replaceAll("\\}", "%7D");
			result = result.replaceAll("\\|", "%7C");
			result = result.replaceAll("\\\\", "%5C");
			result = result.replaceAll("\\^", "%5E");
			result = result.replaceAll("~", "%7E");
			result = result.replaceAll("\\[", "%5B");
			result = result.replaceAll("\\]", "%5D");
			result = result.replaceAll("`", "%60");
		}
		result;
	}

	def encodeReservedChars(originalValue:String ) : String = {
		var result = originalValue; 
		if(result != null) {
			result = result.replaceAll("\\$", "%24");
			result = result.replaceAll("&", "%26");
			result = result.replaceAll("\\+", "%2B");
			result = result.replaceAll(",", "%2C");
			result = result.replaceAll("/", "%2F");
			result = result.replaceAll(":", "%3A");
			result = result.replaceAll(";", "%3B");
			result = result.replaceAll("=", "%3D");
			result = result.replaceAll("\\?", "%3F");
			result = result.replaceAll("@", "%40");
		}
		result;
	}

	val elementContentEntities = Pattern.compile( "<|>|&|[\0-\37&&[^\n\t]]|\uFFFF|\uFFFE" );
		/**
        Answer <code>s</code> modified to replace &lt;, &gt;, and &amp; by
        their corresponding entity references. 
    <p>
        Implementation note: as a (possibly misguided) performance hack, 
        the obvious cascade of replaceAll calls is replaced by an explicit
        loop that looks for all three special characters at once.
	 */
	def substituteEntitiesInElementContent( s:String  ) : String =	{
		val m = elementContentEntities.matcher( s );
		if (!m.find())
			return s;
		else
		{
			var start = 0;
			var result = new StringBuffer();
			do
			{
				result.append( s.substring( start, m.start() ) );
				val ch = s.charAt( m.start() );
				ch match {
					case '\r' => {result.append( "&#xD;" );}
					case '<' => {result.append( "&lt;" ); }
					case '&' => {result.append( "&amp;" ); }
					case '>' => {result.append( "&gt;" ); }
					case _ => { throw new CannotEncodeCharacterException( ch, "XML" );}				  
				}
				start = m.end();
			} while (m.find( start ));
			result.append( s.substring( start ) );
			return result.toString();
		}
	}
	
	def readFileAsString(filePath:String) : String = {
	  val source = scala.io.Source.fromFile(filePath)
	  val result = source.getLines.mkString
	  result;
	}
	
	def readFileAsLines(filePath:String) : java.util.List[String] = {
	  val source = scala.io.Source.fromFile(filePath)
	  val result = source.getLines.toList
	  result;
	}	
}
