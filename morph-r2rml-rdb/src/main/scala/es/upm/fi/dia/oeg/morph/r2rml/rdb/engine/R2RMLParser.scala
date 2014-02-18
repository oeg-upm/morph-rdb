package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument
import es.upm.fi.dia.oeg.obdi.core.engine.AbstractParser
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLMappingDocument

class R2RMLParser extends AbstractParser {
	override def parse(mappingResource:Object ) : AbstractMappingDocument = {
		val mappingDocumentPath = mappingResource.asInstanceOf[String];
		val md = new R2RMLMappingDocument(mappingDocumentPath, null); 
		return md;
	}
}