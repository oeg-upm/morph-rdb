package es.upm.fi.dia.oeg.morph.r2rml.model

import es.upm.fi.dia.oeg.morph.base.RegexUtility
import scala.collection.JavaConversions._

trait ITemplateTermMap {
	var templateString:String =null;
	
	def getTemplateString():String = { this.templateString };
	
	def getTemplateColumns():Iterable[String] = {
      RegexUtility.getTemplateColumns(this.templateString, true);
   };
	
	def getTemplateValues(uri:String ) : Map[String, String]  = {
		RegexUtility.getTemplateMatching(this.templateString, uri);
	}
}