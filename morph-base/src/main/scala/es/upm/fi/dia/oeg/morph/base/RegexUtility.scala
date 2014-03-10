package es.upm.fi.dia.oeg.morph.base

import scala.util.matching.Regex
import scala.collection.JavaConversions._
import java.util.regex.Matcher
import java.util.regex.Pattern

object RegexUtility {
	val patternString1 = Constants.R2RML_TEMPLATE_PATTERN;
	val TemplatePattern1 = patternString1.r;
  
	def main(args:Array[String]) = {
		val template = "Hello \\{ {Name} \\} Please find attached {Invoice Number} which is due on {Due Date}";
		
		val replacements = Map("Name" -> "Freddy", "Invoice Number" -> "INV0001")
		
		val attributes = RegexUtility.getTemplateColumns(template, true);
		System.out.println("attributes = " + attributes);
		
		val template2 = RegexUtility.replaceTokens(template, replacements);
		System.out.println("template2 = " + template2);	  
	}
	
	def getTemplateMatching(inputTemplateString: String, inputURIString : String) 
	: Map[String, String] = {
	  var newTemplateString = inputTemplateString;
	  if (!newTemplateString.startsWith("<")) {
	    newTemplateString = "<" + newTemplateString;
	  }
	  if(!newTemplateString.endsWith(">")) {
	    newTemplateString = newTemplateString + ">";
	  }
	  
	  var newURIString = inputURIString;
	  if(!newURIString.startsWith("<")) {
	    newURIString = "<" + newURIString;
	  }
	  if(!newURIString.endsWith(">")) {
	    newURIString = newURIString + ">";
	  }
	  
		//val result:Map[String, String] = Map.empty;
		val columnsFromTemplate = this.getTemplateColumns(newTemplateString, false);
		//println("columnsFromTemplate = " + columnsFromTemplate);
		
		var columnsList = List[String]();
		var templateString1 = newTemplateString; 
		for(column <- columnsFromTemplate) {
			val column2 = column.substring(1, column.length() - 1);
			columnsList = column2 :: columnsList; 
			templateString1 = templateString1.replaceAll("\\{" + column2 + "\\}", "(.+?)");
		}
		columnsList = columnsList.reverse;
		//println("columnsList = " + columnsList);

				
		val TemplatePattern = templateString1.r;
		val pattern = new Regex(templateString1);
		val firstMatch = pattern.findFirstMatchIn(newURIString);
		
		val result : Map[String, String]= if(firstMatch != None) {
			val subgroups = firstMatch.get.subgroups;
			var i = 0;
			columnsList.map(column  => {
			  val resultAux = (column -> subgroups(i));
			  i = i+1;
			  resultAux
			}).toMap
		} else {
		  Map.empty
		}
		
		result;
	}
	
	def getTemplateColumns(templateString0 : String, cleanColumn : Boolean) : java.util.List[String] = {
		val columnsFromTemplate = TemplatePattern1.findAllIn(templateString0).toList;
		val result = { if(cleanColumn) {
		  for(templateColumn <- columnsFromTemplate) 
		    yield templateColumn.substring(1, templateColumn.length() - 1)
		} else {columnsFromTemplate} }
		result;
	}
	
	def replaceTokens(matcher:Matcher, text: String, replacements:java.util.Map[String, Object] ) 
	: String = {
		var buffer:StringBuffer = new StringBuffer();
		while (matcher.find()) {
			val matcherGroup1 = matcher.group(1);
			val replacement = replacements.get(matcherGroup1);
			if (replacement != null) {
				matcher.appendReplacement(buffer, "");
				buffer.append(replacement);
			}
		}
		matcher.appendTail(buffer);
		return buffer.toString();		
	}
	
	def replaceTokens(pText:String, replacements:Map[String, Object] ) : String  = {
	  if(replacements != null && !replacements.isEmpty) {
		val text = pText.replaceAll("\\\\\\{", "morphopencurly")
				.replaceAll("\\\\\\}", "morphclosecurly");
		
		val pattern = Pattern.compile("\\{(.+?)\\}");
		val matcher = pattern.matcher(text);
		
		val replacedToken = RegexUtility.replaceTokens(matcher, text, replacements)
			.replaceAll("morphopencurly", "\\{")
			.replaceAll("morphclosecurly", "\\}");

		return replacedToken;	    
	  } else { null }

	}	
}