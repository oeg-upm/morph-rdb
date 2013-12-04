package es.upm.fi.dia.oeg.morph.base

import scala.util.matching.Regex
import scala.collection.JavaConversions._
import java.util.HashMap

object RegexUtility {
	val patternString1 = "\\{\\w+\\}";
	val TemplatePattern1 = patternString1.r;
  
	def getTemplateMatching(inputTemplateString: String, inputURIString : String) : HashMap[String, String] = {
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
	  
		val result = new HashMap[String, String];
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
		
		if(firstMatch != None) {
			val subgroups = firstMatch.get.subgroups;
			var i = 0;
			val columnsListIterator = columnsList.iterator;
			while(columnsListIterator.hasNext) {
			  val column = columnsListIterator.next;
			  	//println("column = " + column);
				result += column -> subgroups(i);
				i = i+1;
			}
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
}