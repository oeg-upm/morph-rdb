package es.upm.fi.dia.oeg.obdi.core.engine;

import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument;


public abstract class AbstractParser {
	public abstract AbstractMappingDocument parse(Object mappingResource) throws Exception; 
}
