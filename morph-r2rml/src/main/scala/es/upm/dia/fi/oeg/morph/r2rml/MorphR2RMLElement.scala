package es.upm.dia.fi.oeg.morph.r2rml

trait MorphR2RMLElement {
	def accept(visitor:MorphR2RMLElementVisitor) : Object;
}