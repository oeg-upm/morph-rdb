package es.upm.fi.dia.oeg.core.utility.test

import es.upm.fi.dia.oeg.morph.base.TriplePatternPredicateBounder


object MorphValidatorTest extends App {
	val mappingFile = "https://dl.dropboxusercontent.com/u/531378/bsbm/bsbm.r2rml.ttl";
	val morphValidatorTest = new TriplePatternPredicateBounder(mappingFile, null);
	println("Bye");
}