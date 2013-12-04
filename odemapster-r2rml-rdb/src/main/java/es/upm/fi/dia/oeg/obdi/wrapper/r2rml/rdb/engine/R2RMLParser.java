package es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.engine;

import es.upm.fi.dia.oeg.obdi.core.engine.AbstractParser;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLMappingDocument;

public class R2RMLParser extends AbstractParser {

	@Override
	public AbstractMappingDocument parse(Object mappingResource) throws Exception {
		String mappingDocumentPath = (String) mappingResource;
		R2RMLMappingDocument md = new R2RMLMappingDocument(mappingDocumentPath, null); 
		return md;
	}

}
