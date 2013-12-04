package es.upm.fi.dia.oeg.morph.querytranslator.test;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Test;

import es.upm.fi.dia.oeg.morph.base.Constants;
import es.upm.fi.dia.oeg.obdi.core.ConfigurationProperties;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.engine.R2RMLRunner;

public class ExampleWithoutPropertiesFile {
	private static Logger logger = Logger.getLogger(ExampleWithPropertiesFile.class);
	private String jdbc_url = "jdbc:mysql://127.0.0.1:3306/morph_example";
	private String dbUserName = "root";
	private String dbPassword = "";
	private String dbName = "morph_example";
	private String databaseDriver = "com.mysql.jdbc.Driver";
	private String databaseType = Constants.DATABASE_MYSQL();
	
	static {
		PropertyConfigurator.configure("log4j.properties");
	}

	@Test
	public void sparql01FromCode() {
		String queryFile = System.getProperty("user.dir") 
				+ File.separator + "examples" + File.separator + "query01.sparql";
		String resultFile = System.getProperty("user.dir") 
				+ File.separator + "examples" + File.separator + "query01FromCode-result.xml";
		String mappingDocumentFile = System.getProperty("user.dir") 
				+ File.separator + "examples" + File.separator + "example.ttl";
		
		ConfigurationProperties properties = new ConfigurationProperties();
		properties.setNoOfDatabase(1);
		properties.setDatabaseUser(dbUserName);
		properties.setDatabasePassword(dbPassword);
		properties.setDatabaseName(dbName);
		properties.setDatabaseURL(jdbc_url);
		properties.setDatabaseDriver(databaseDriver);
		properties.setDatabaseType(databaseType);
		properties.setMappingDocumentFilePath(mappingDocumentFile);
		properties.setOutputFilePath(resultFile);
		assertTrue("sparql01 done", true);
		try {
			R2RMLRunner runner = new R2RMLRunner(properties);
			runner.readSPARQLFile(queryFile);
			runner.run();
		} catch(Exception e) {
			e.printStackTrace();
			String errorMessage = "Error occured: " + e.getMessage();
			logger.warn(errorMessage);
			assertTrue(e.getMessage(), false);
		}
	}
}
