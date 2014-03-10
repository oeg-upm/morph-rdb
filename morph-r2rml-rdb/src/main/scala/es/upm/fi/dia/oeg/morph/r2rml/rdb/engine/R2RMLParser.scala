package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import scala.collection.JavaConversions._
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLMappingDocument
import com.hp.hpl.jena.rdf.model.Resource
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLSubjectMap
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLSubjectMap
import es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLPredicateMap
import es.upm.dia.fi.oeg.morph.r2rml.MorphR2RMLElementVisitor
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseParser

class R2RMLParser extends MorphBaseParser with MorphR2RMLElementVisitor {
	override def parse(mappingResource:Object ) : MorphBaseMappingDocument = {
		val mappingDocumentPath = mappingResource.asInstanceOf[String];
		val md = R2RMLMappingDocument(mappingDocumentPath); 
		return md;
	}

	def visit(termMap: es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLTermMap): Object = {null}   
	def visit(rom: es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLRefObjectMap): Object = {null}   
	def visit(om: es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLObjectMap): Object = {null}   
	def visit(logicalTable: es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLLogicalTable): Object = {null}   
	def visit(tm: es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLTriplesMap): Object = {null}   
	def visit(md: es.upm.dia.fi.oeg.morph.r2rml.model.R2RMLMappingDocument): Object = {null} 

}

