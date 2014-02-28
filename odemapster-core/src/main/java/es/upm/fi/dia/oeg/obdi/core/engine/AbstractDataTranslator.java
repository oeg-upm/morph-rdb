package es.upm.fi.dia.oeg.obdi.core.engine;

import java.io.IOException;
import java.sql.Connection;
import java.util.Collection;

import org.apache.log4j.Logger;

import es.upm.fi.dia.oeg.morph.base.ConfigurationProperties;
import es.upm.fi.dia.oeg.obdi.core.exception.PostProcessorException;
import es.upm.fi.dia.oeg.obdi.core.materializer.AbstractMaterializer;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument;

public abstract class AbstractDataTranslator {
	private static Logger logger = Logger.getLogger(AbstractDataTranslator.class);
	protected ConfigurationProperties properties;
	protected AbstractMaterializer materializer;
	protected AbstractUnfolder unfolder;
	protected AbstractRunner runner;
	protected Connection connection;
	
	public AbstractDataTranslator(ConfigurationProperties properties
			, AbstractMaterializer materializer, AbstractUnfolder unfolder
			, AbstractRunner runner, Connection connection) {
		this.properties = properties;
		this.materializer = materializer;
		this.unfolder = unfolder;
		this.runner= runner;
		this.connection = connection;
	}
	
	public AbstractDataTranslator(ConfigurationProperties properties) {
		try {
			this.properties = properties;
			
			String outputFileName = properties.outputFilePath();
			String rdfLanguage = properties.rdfLanguage();
			String jenaMode = properties.jenaMode();
			this.materializer = AbstractMaterializer.create(rdfLanguage, outputFileName, jenaMode);
		} catch (IOException e) {
			logger.error("IO error while loading configuration file : " + properties.configurationFileURL());
			logger.error("error message = " + e.getMessage());
			e.printStackTrace();
		} catch (Exception e) {
			logger.error("Error while loading configuration file : " + properties.configurationFileURL());
			logger.error("error message = " + e.getMessage());
			e.printStackTrace();
		}	
	}
	
//	public AbstractDataTranslator(String configurationDirectory
//			, String configurationFile) {
//		try {
//			this.properties = new ConfigurationProperties(
//					configurationDirectory, configurationFile);
//			
//			String outputFileName = properties.getOutputFilePath();
//			String rdfLanguage = properties.getRdfLanguage();
//			String jenaMode = properties.getJenaMode();
//			this.materializer = AbstractMaterializer.create(rdfLanguage, outputFileName, jenaMode);
//		} catch (IOException e) {
//			logger.error("IO error while loading configuration file : " + configurationFile);
//			logger.error("error message = " + e.getMessage());
//			e.printStackTrace();
//		} catch (InvalidConfigurationPropertiesException e) {
//			logger.error("invalid configuration error while loading configuration file : " + configurationFile);
//			logger.error("error message = " + e.getMessage());
//			e.printStackTrace();
//		} catch (SQLException e) {
//			logger.error("Database error while loading configuration file : " + configurationFile);
//			logger.error("error message = " + e.getMessage());
//			e.printStackTrace();
//		} catch (Exception e) {
//			logger.error("Error while loading configuration file : " + configurationFile);
//			logger.error("error message = " + e.getMessage());
//			e.printStackTrace();
//		}
//	}
	
	protected abstract Object processCustomFunctionTransformationExpression(Object argument) 
			throws PostProcessorException;
	public abstract void setMaterializer(AbstractMaterializer materializer);
	public abstract void translateData(AbstractMappingDocument mappingDocument) throws Exception;
	public abstract void generateRDFTriples(AbstractConceptMapping cm, String sqlQuery) throws Exception;
	public abstract void generateSubjects(AbstractConceptMapping cm, String sqlQuery) throws Exception;
	
	public void setUnfolder(AbstractUnfolder unfolder) {
		this.unfolder = unfolder;
	}

	public abstract void translateData(Collection<AbstractConceptMapping> triplesMaps)
			throws Exception;
	
//	public void setUnfolder(AbstractUnfolder unfolder) {
//		this.unfolder = unfolder;
//	}
	
}
