package es.upm.fi.dia.oeg.morph.r2rml.engine

import scala.collection.JavaConversions._
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.r2rml.MorphR2RMLElementVisitor
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseParser
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLRefObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLLogicalTable
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap

class R2RMLParser extends MorphBaseParser with MorphR2RMLElementVisitor {
	override def parse(mappingResource:Object ) : MorphBaseMappingDocument = {
		val mappingDocumentPath = mappingResource.asInstanceOf[String];
		val md = R2RMLMappingDocument(mappingDocumentPath); 
		return md;
	}

	def visit(termMap: R2RMLTermMap): Object = {null}   
	def visit(rom: R2RMLRefObjectMap): Object = {null}   
	def visit(om: R2RMLObjectMap): Object = {null}   
	def visit(logicalTable: R2RMLLogicalTable): Object = {null}   
	def visit(tm: R2RMLTriplesMap): Object = {null}   
	def visit(md: R2RMLMappingDocument): Object = {null} 

}

