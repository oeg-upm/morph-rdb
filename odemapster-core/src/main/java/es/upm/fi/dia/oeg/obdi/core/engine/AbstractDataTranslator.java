package es.upm.fi.dia.oeg.obdi.core.engine;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import es.upm.fi.dia.oeg.obdi.core.ConfigurationProperties;
import es.upm.fi.dia.oeg.obdi.core.exception.InvalidConfigurationPropertiesException;
import es.upm.fi.dia.oeg.obdi.core.exception.PostProcessorException;
import es.upm.fi.dia.oeg.obdi.core.materializer.AbstractMaterializer;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument;

public abstract class AbstractDataTranslator {
	private static Logger logger = Logger.getLogger(AbstractDataTranslator.class);
	protected AbstractMaterializer materializer;
	protected ConfigurationProperties properties;
	protected AbstractUnfolder unfolder;
	protected AbstractRunner owner;
		
	public AbstractDataTranslator(ConfigurationProperties properties) {
		this.properties = properties;
	}
	
	public AbstractDataTranslator(String configurationDirectory
			, String configurationFile) {
		try {
			this.properties = new ConfigurationProperties(
					configurationDirectory, configurationFile);
			
			String outputFileName = properties.getOutputFilePath();
			String rdfLanguage = properties.getRdfLanguage();
			String jenaMode = properties.getJenaMode();
			this.materializer = AbstractMaterializer.create(rdfLanguage, outputFileName, jenaMode);
		} catch (IOException e) {
			logger.error("IO error while loading configuration file : " + configurationFile);
			logger.error("error message = " + e.getMessage());
			e.printStackTrace();
		} catch (InvalidConfigurationPropertiesException e) {
			logger.error("invalid configuration error while loading configuration file : " + configurationFile);
			logger.error("error message = " + e.getMessage());
			e.printStackTrace();
		} catch (SQLException e) {
			logger.error("Database error while loading configuration file : " + configurationFile);
			logger.error("error message = " + e.getMessage());
			e.printStackTrace();
		} catch (Exception e) {
			logger.error("Error while loading configuration file : " + configurationFile);
			logger.error("error message = " + e.getMessage());
			e.printStackTrace();
		}
	}
	
	protected abstract Object processCustomFunctionTransformationExpression(Object argument) 
			throws PostProcessorException;
	public abstract void setMaterializer(AbstractMaterializer materializer);
	public abstract void translateData(AbstractMappingDocument mappingDocument) throws Exception;

	public void setUnfolder(AbstractUnfolder unfolder) {
		this.unfolder = unfolder;
	}
	
}
