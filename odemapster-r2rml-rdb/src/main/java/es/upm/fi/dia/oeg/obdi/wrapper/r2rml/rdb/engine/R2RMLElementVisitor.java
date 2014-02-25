package es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.engine;

import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLLogicalTable;
//import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLObjectMap;
//import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLRefObjectMap;
//import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTermMap;

public interface R2RMLElementVisitor {
	public Object visit(AbstractMappingDocument mappingDocument) throws Exception;
	public Object visit(AbstractConceptMapping classMapping) throws Exception;
	public Object visit(R2RMLLogicalTable logicalTable);
//	public Object visit(R2RMLObjectMap objectMap);
//	public Object visit(R2RMLRefObjectMap refObjectMap);
	//public Object visit(R2RMLTermMap r2rmlTermMap);
}
