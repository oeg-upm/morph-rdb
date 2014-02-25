package es.upm.fi.dia.oeg.obdi.core.engine;

import java.sql.Connection;

import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument;

public interface IQueryTranslatorFactory {
	public IQueryTranslator createQueryTranslator(AbstractMappingDocument mappingDocument
			, Connection conn, AbstractUnfolder unfolder);
}
