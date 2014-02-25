package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument
import java.sql.Connection
import es.upm.fi.dia.oeg.obdi.core.engine.IQueryTranslator
import es.upm.fi.dia.oeg.obdi.core.engine.AbstractUnfolder

trait IQueryTranslatorFactory {
	def createQueryTranslator(mappingDocument:AbstractMappingDocument
	    , conn:Connection, unfolder:AbstractUnfolder) : IQueryTranslator;
}