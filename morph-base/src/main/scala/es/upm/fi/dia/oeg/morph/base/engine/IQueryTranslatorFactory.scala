package es.upm.fi.dia.oeg.morph.base.engine

import java.sql.Connection
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.MorphProperties

trait IQueryTranslatorFactory {
	def createQueryTranslator(mappingDocument:MorphBaseMappingDocument
	    , conn:Connection, properties:MorphProperties) : IQueryTranslator;
}