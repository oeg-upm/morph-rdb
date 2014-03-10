package es.upm.fi.dia.oeg.morph.base.engine

import java.sql.Connection
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument

trait IQueryTranslatorFactory {
	def createQueryTranslator(mappingDocument:MorphBaseMappingDocument
	    , conn:Connection) : IQueryTranslator;
}