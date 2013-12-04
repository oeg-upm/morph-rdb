package es.upm.fi.dia.oeg.core.utility.test

import scala.util.matching.Regex
import scala.collection.mutable.HashMap
import es.upm.fi.dia.oeg.morph.base.RegexUtility

object RegexUtilityTest extends App {
	
	val templateString0 = "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature/{nr}";
	println("templateString0 = " + templateString0);

	val templateColumns = RegexUtility.getTemplateColumns(templateString0, true);
	println("templateColumns = " + templateColumns);
	
	val uriString = "http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductFeature/168";
	println("uriString = " + uriString);
	
	
	val templateValues = RegexUtility.getTemplateMatching(templateString0, uriString);
	println("templateValues = " + templateValues);
	
}