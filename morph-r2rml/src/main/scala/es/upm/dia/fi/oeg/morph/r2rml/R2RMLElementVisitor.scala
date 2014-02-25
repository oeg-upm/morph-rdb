package es.upm.dia.fi.oeg.morph.r2rml

import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLLogicalTable

trait R2RMLElementVisitor {
	def visit(md:AbstractMappingDocument ) : Object;
	def visit(cm:AbstractConceptMapping ) : Object;
	def visit(logicalTable:R2RMLLogicalTable ) : Object ;
//	def visit(om:R2RMLObjectMap ) : Object ;
//	def visit(rom:R2RMLRefObjectMap):Object ;
	//def visit(termMap:R2RMLTermMap) : Object ;
}