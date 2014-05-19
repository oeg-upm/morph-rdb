package es.upm.fi.dia.oeg.morph.r2rml

import es.upm.fi.dia.oeg.morph.r2rml.MorphR2RMLElementVisitor

trait MorphR2RMLElement {
	def accept(visitor:MorphR2RMLElementVisitor) : Object;
}