package es.upm.fi.dia.oeg.morph.example;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.sql.Connection;
import java.util.Properties;

import org.junit.Test;

import es.upm.fi.dia.oeg.morph.base.Constants;
import es.upm.fi.dia.oeg.morph.base.DBUtility;
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseRunner;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBProperties;
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBRunnerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExampleWithoutPropertiesFile {
	private Logger logger = LoggerFactory.getLogger(this.getClass());

	//static { PropertyConfigurator.configure("log4j.properties"); }
	
	private String jdbc_url = "jdbc:mysql://127.0.0.1:3306/morph_example";
	private String dbUserName = "root";
	private String dbPassword = "";
	private String dbName = "morph_example";
	private String databaseDriver = "com.mysql.jdbc.Driver";
	private String databaseType = Constants.DATABASE_MYSQL();
	
//	static {
//		PropertyConfigurator.configure("log4j.properties");
//	}

	@Test
	public void testBatch() {
		String resultFile = System.getProperty("user.dir") 
				+ File.separator + "examples" + File.separator + "batch-result.nt";
		String mappingDocumentFile = System.getProperty("user.dir") 
				+ File.separator + "examples" + File.separator + "example.ttl";
		
		MorphRDBProperties properties = new MorphRDBProperties();
		properties.setNoOfDatabase(1);
		properties.setDatabaseUser(dbUserName);
		properties.setDatabasePassword(dbPassword);
		properties.setDatabaseName(dbName);
		properties.setDatabaseURL(jdbc_url);
		properties.setDatabaseDriver(databaseDriver);
		properties.setDatabaseType(databaseType);
		properties.setMappingDocumentFilePath(mappingDocumentFile);
		properties.setOutputFilePath(resultFile);
		try {
			MorphRDBRunnerFactory runnerFactory = new MorphRDBRunnerFactory();
			MorphBaseRunner runner = runnerFactory.createRunner(properties);
			runner.run();
			assertTrue("testBatch done", true);
		} catch(Exception e) {
			e.printStackTrace();
			String errorMessage = "Error occured: " + e.getMessage();
			assertTrue(errorMessage, false);
		}
	}
	
	@Test
	public void sparql01FromCode() {
		String queryFile = System.getProperty("user.dir") 
				+ File.separator + "examples" + File.separator + "query01.sparql";
		String resultFile = System.getProperty("user.dir") 
				+ File.separator + "examples" + File.separator + "query01FromCode-result.xml";
		String mappingDocumentFile = System.getProperty("user.dir") 
				+ File.separator + "examples" + File.separator + "example.ttl";
		
		MorphRDBProperties properties = new MorphRDBProperties();
		properties.setNoOfDatabase(1);
		properties.setDatabaseUser(dbUserName);
		properties.setDatabasePassword(dbPassword);
		properties.setDatabaseName(dbName);
		properties.setDatabaseURL(jdbc_url);
		properties.setDatabaseDriver(databaseDriver);
		properties.setDatabaseType(databaseType);
		properties.setMappingDocumentFilePath(mappingDocumentFile);
		properties.setOutputFilePath(resultFile);
		try {
			MorphRDBRunnerFactory runnerFactory = new MorphRDBRunnerFactory();
			MorphBaseRunner runner = runnerFactory.createRunner(properties);
			runner.readSPARQLFile(queryFile);
			runner.run();
			assertTrue("sparql01 done", true);
		} catch(Exception e) {
			e.printStackTrace();
			String errorMessage = "Error occured: " + e.getMessage();
			assertTrue(errorMessage, false);
		}
	}
	

	

	
	@Test
	public void testQueryGetSubjects() {
		String queryFile = System.getProperty("user.dir") 
				+ File.separator + "examples" + File.separator + "query11.sparql";
		String resultFile = System.getProperty("user.dir") 
				+ File.separator + "examples" + File.separator + "query11FromCode-result.xml";
		String mappingDocumentFile = System.getProperty("user.dir") 
				+ File.separator + "examples" + File.separator + "example.ttl";
		
		MorphRDBProperties properties = new MorphRDBProperties();
		properties.setNoOfDatabase(1);
		properties.setDatabaseUser(dbUserName);
		properties.setDatabasePassword(dbPassword);
		properties.setDatabaseName(dbName);
		properties.setDatabaseURL(jdbc_url);
		properties.setDatabaseDriver(databaseDriver);
		properties.setDatabaseType(databaseType);
		properties.setMappingDocumentFilePath(mappingDocumentFile);
		properties.setOutputFilePath(resultFile);
		try {
			MorphRDBRunnerFactory runnerFactory = new MorphRDBRunnerFactory();
			MorphBaseRunner runner = runnerFactory.createRunner(properties);
			runner.readSPARQLFile(queryFile);
			runner.run();
			assertTrue("sparql01 done", true);
		} catch(Exception e) {
			e.printStackTrace();
			String errorMessage = "Error occured: " + e.getMessage();
			assertTrue(errorMessage, false);
		}
	}

	@Test
	public void testQueryGetSubjectDetails() {
		String queryFile = System.getProperty("user.dir") 
				+ File.separator + "examples" + File.separator + "query12.sparql";
		String resultFile = System.getProperty("user.dir") 
				+ File.separator + "examples" + File.separator + "query12FromCode-result.xml";
		String mappingDocumentFile = System.getProperty("user.dir") 
				+ File.separator + "examples" + File.separator + "example.ttl";
		
		MorphRDBProperties properties = new MorphRDBProperties();
		properties.setNoOfDatabase(1);
		properties.setDatabaseUser(dbUserName);
		properties.setDatabasePassword(dbPassword);
		properties.setDatabaseName(dbName);
		properties.setDatabaseURL(jdbc_url);
		properties.setDatabaseDriver(databaseDriver);
		properties.setDatabaseType(databaseType);
		properties.setMappingDocumentFilePath(mappingDocumentFile);
		properties.setOutputFilePath(resultFile);
		try {
			MorphRDBRunnerFactory runnerFactory = new MorphRDBRunnerFactory();
			MorphBaseRunner runner = runnerFactory.createRunner(properties);
			runner.readSPARQLFile(queryFile);
			runner.run();
			assertTrue("sparql01 done", true);
		} catch(Exception e) {
			e.printStackTrace();
			String errorMessage = "Error occured: " + e.getMessage();
			assertTrue(errorMessage, false);
		}
	}

	@Test
	public void testIASoftWithoutPropertiesFile() {
		String driverString = "oracle.jdbc.OracleDriver";
		String url = "jdbc:oracle:thin:@localhost:1521:xe";
		String username = "";
		String password = "";
		String databaseName = "";

		Properties prop = new Properties();
		prop.put("ResultSetMetaDataOptions", "1");
		prop.put("user", username);
		prop.put("password", password);
		prop.put("autoReconnect", "true");
		Connection conn = null;
		try {
			Class.forName(driverString);
			conn = DBUtility.getLocalConnection(username, databaseName, password, driverString, url
					, "testIASoft");
			logger.info("Connection obtained.");
		} catch (Exception e) {
			logger.error("Error while obtaining connection: " + e.getMessage());
			e.printStackTrace();
			assertTrue(false);
		}

		String configurationDirectory = "C:/Users/fpriyatna/Documents/dodga/iasoft/morph-files";
		String mappingDocumentFile = configurationDirectory + File.separator + "pproc5.ttl";
		String resultFile = configurationDirectory + File.separator + "pproc5-result.nt";
		MorphRDBProperties properties = new MorphRDBProperties();
		properties.setMappingDocumentFilePath(mappingDocumentFile);
		properties.setOutputFilePath(resultFile);
		properties.setDatabaseType(Constants.DATABASE_ORACLE());
		try {
			MorphRDBRunnerFactory runnerFactory = new MorphRDBRunnerFactory();
			MorphBaseRunner runner = runnerFactory.createRunner(conn, properties);
			runner.run();
			System.out.println("Batch process DONE------\n\n");
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Batch process FAILED------\n\n");
			assertTrue(e.getMessage(), false);
		}
	}
}
