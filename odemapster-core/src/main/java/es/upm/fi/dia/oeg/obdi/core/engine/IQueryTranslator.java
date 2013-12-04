package es.upm.fi.dia.oeg.obdi.core.engine;

import java.sql.Connection;

import com.hp.hpl.jena.query.Query;

import es.upm.fi.dia.oeg.obdi.core.ConfigurationProperties;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument;
import es.upm.fi.dia.oeg.obdi.core.sql.IQuery;

public interface IQueryTranslator {
	Connection getConnection();
	
	void setOptimizer(IQueryTranslationOptimizer optimizer);

	Query getSPARQLQuery();
	
	void setSPARQLQuery(Query query);
	
	void setSPARQLQueryByString(String queryString);
	
	void setSPARQLQueryByFile(String queryFilePath);
	
	IQuery translate(Query query) throws Exception;

	void setMappingDocument(AbstractMappingDocument md);
	
	AbstractMappingDocument getMappingDocument();

	void setUnfolder(AbstractUnfolder unfolder);

	void setIgnoreRDFTypeStatement(boolean b);

	IQuery translateFromQueryFile(String queryFilePath) throws Exception;

	IQueryTranslationOptimizer getOptimizer();

	IQuery translateFromString(String queryString) throws Exception ;

	//String translateResultSet(String columnLabel, String dbValue);
	
	String translateResultSet(String varName, AbstractResultSet rs);
	
	void setConfigurationProperties(ConfigurationProperties configurationProperties);

	void setDatabaseType(String databaseType);

	void setConnection(Connection conn);
}
