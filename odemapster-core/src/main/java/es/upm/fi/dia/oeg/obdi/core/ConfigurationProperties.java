package es.upm.fi.dia.oeg.obdi.core;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.log4j.Logger;

import es.upm.fi.dia.oeg.morph.base.Constants;
import es.upm.fi.dia.oeg.obdi.core.exception.InvalidConfigurationPropertiesException;

public class ConfigurationProperties extends Properties {
	
	//change this to typesafe config

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Logger logger = Logger.getLogger(ConfigurationProperties.class);

	private Connection conn;
	private String ontologyFilePath;
	private String mappingDocumentFilePath;
	private String outputFilePath;
	private String queryFilePath;
	private String rdfLanguage;
	private String jenaMode;
	private String databaseType;

	//query translator
	private String queryTranslatorClassName;
	private String queryEvaluatorClassName;
	private String queryResultWriterClassName;

	//query optimizer
	private boolean reorderSTG = true;
	private boolean selfJoinElimination = true;
	private boolean subQueryElimination = true;
	private boolean transJoinSubQueryElimination = true;
	private boolean transSTGSubQueryElimination = true;
	private boolean subQueryAsView = false;
	

	//batch upgrade
	private boolean literalRemoveStrangeChars;
	private boolean encodeUnsafeChars;
	private boolean encodeReservedChars;

	//database
	private int noOfDatabase;
	private String databaseDriver; 
	private String databaseURL;
	private String databaseName;
	private String databaseUser;
	private String databasePassword;
	private int databaseTimeout = 0;

	public boolean isSelfJoinElimination() {
		return selfJoinElimination;
	}

	public ConfigurationProperties() {}


	public ConfigurationProperties(
			String configurationDirectory, String configurationFile) 
					throws Exception 
					{
		String absoluteConfigurationFile = configurationFile;
		if(configurationDirectory != null) {
			if(!configurationDirectory.endsWith("/")) {
				configurationDirectory = configurationDirectory + "/";
			}
			absoluteConfigurationFile = configurationDirectory + configurationFile; 
		}
		logger.info("reading configuration file : " + absoluteConfigurationFile);
		try {
			this.load(new FileInputStream(absoluteConfigurationFile));
		} catch (FileNotFoundException e) {
			String errorMessage = "Configuration file not found: " + absoluteConfigurationFile;
			logger.error(errorMessage);
			e.printStackTrace();
			throw e;
		} catch (IOException e) {
			String errorMessage = "Error reading configuration file: " + absoluteConfigurationFile;
			logger.error(errorMessage);
			e.printStackTrace();
			throw e;
		}

		this.readConfigurationFile(configurationDirectory);
					}

	public String getOutputFilePath() {
		return outputFilePath;
	}

	public String getMappingDocumentFilePath() {
		return mappingDocumentFilePath;
	}

	private void readConfigurationFile(String configurationDir) 
			throws Exception {
		
		this.noOfDatabase = this.readInteger(Constants.NO_OF_DATABASE_NAME_PROP_NAME(), 0); 
		if(this.noOfDatabase != 0 && this.noOfDatabase != 1) {
			throw new InvalidConfigurationPropertiesException("Only zero or one database is supported.");
		}

		this.conn = null;
		for(int i=0; i<noOfDatabase;i++) {
			String propertyDatabaseDriver = Constants.DATABASE_DRIVER_PROP_NAME() + "[" + i + "]";
			this.databaseDriver = this.getProperty(propertyDatabaseDriver);

			String propertyDatabaseURL = Constants.DATABASE_URL_PROP_NAME() + "[" + i + "]";
			this.databaseURL = this.getProperty(propertyDatabaseURL);

			String propertyDatabaseName= Constants.DATABASE_NAME_PROP_NAME() + "[" + i + "]";
			this.databaseName = this.getProperty(propertyDatabaseName);

			String propertyDatabaseUser = Constants.DATABASE_USER_PROP_NAME() + "[" + i + "]";
			this.databaseUser = this.getProperty(propertyDatabaseUser);

			String propertyDatabasePassword = Constants.DATABASE_PWD_PROP_NAME()  + "[" + i + "]";
			this.databasePassword = this.getProperty(propertyDatabasePassword);

			String propertyDatabaseType = Constants.DATABASE_TYPE_PROP_NAME()  + "[" + i + "]";
			this.databaseType = this.getProperty(propertyDatabaseType);

			String propertyDatabaseTimeout = Constants.DATABASE_TIMEOUT_PROP_NAME()  + "[" + i + "]";
			String timeoutPropertyString = this.getProperty(propertyDatabaseTimeout);
			if(timeoutPropertyString != null && !timeoutPropertyString.equals("")) {
				this.databaseTimeout = Integer.parseInt(timeoutPropertyString.trim());
			}

			logger.info("Obtaining database connection...");
			try {
				this.conn = DBUtility.getLocalConnection(
						databaseUser, databaseName, databasePassword, 
						databaseDriver, 
						databaseURL, "Configuration Properties");
				if(this.conn != null) {
					logger.info("Connection obtained.");
				}
			} catch (SQLException e) {
				this.conn = null;
				String errorMessage = "Error loading database, error message = " + e.getMessage();
				logger.error(errorMessage);
				//e.printStackTrace();
			}
		}

		this.mappingDocumentFilePath = this.readString(Constants.MAPPINGDOCUMENT_FILE_PATH(), null);
		if(this.mappingDocumentFilePath != null) {
			boolean isNetResourceMapping = ODEMapsterUtility.isNetResource(this.mappingDocumentFilePath);
			if(!isNetResourceMapping && configurationDir != null) {
				this.mappingDocumentFilePath = configurationDir + mappingDocumentFilePath;
			}
		}

		this.queryFilePath = this.getProperty(Constants.QUERYFILE_PROP_NAME());
		boolean isNetResourceQuery = ODEMapsterUtility.isNetResource(this.queryFilePath);
		if(!isNetResourceQuery && configurationDir != null) {
			if(this.queryFilePath != null && !this.queryFilePath.equals("")) {
				this.queryFilePath = configurationDir + queryFilePath;
			}
		}

		this.ontologyFilePath = this.getProperty(Constants.ONTOFILE_PROP_NAME());
		this.outputFilePath = this.getProperty(Constants.OUTPUTFILE_PROP_NAME());

		if(configurationDir != null) {

			this.outputFilePath = configurationDir + outputFilePath;

			if(this.ontologyFilePath != null && !this.ontologyFilePath.equals("")) {
				this.ontologyFilePath = configurationDir + ontologyFilePath;
			}

		}

		this.rdfLanguage = this.readString(Constants.OUTPUTFILE_RDF_LANGUAGE()
				, Constants.OUTPUT_FORMAT_NTRIPLE());
		logger.debug("rdf language = " + this.rdfLanguage);

		this.jenaMode = this.readString(Constants.JENA_MODE_TYPE(), Constants.JENA_MODE_TYPE_MEMORY());
		logger.debug("Jena mode = " + jenaMode);

		this.selfJoinElimination = this.readBoolean(Constants.OPTIMIZE_TB(), true);
		logger.debug("Self join elimination = " + this.selfJoinElimination);

		this.reorderSTG = this.readBoolean(Constants.REORDER_STG(), true);
		logger.debug("Reorder STG = " + this.reorderSTG);

		this.subQueryElimination = this.readBoolean(Constants.SUBQUERY_ELIMINATION(), true);
		logger.debug("Subquery elimination = " + this.subQueryElimination);

		this.transJoinSubQueryElimination = this.readBoolean(
				Constants.TRANSJOIN_SUBQUERY_ELIMINATION(), true);
		logger.debug("Trans join subquery elimination = " + this.transJoinSubQueryElimination);

		this.transSTGSubQueryElimination = this.readBoolean(
				Constants.TRANSSTG_SUBQUERY_ELIMINATION(), true);
		logger.debug("Trans stg subquery elimination = " + this.transSTGSubQueryElimination);

		this.subQueryAsView = this.readBoolean(Constants.SUBQUERY_AS_VIEW(), false);
		logger.debug("Subquery as view = " + this.subQueryAsView);

		this.queryTranslatorClassName = this.readString(
				Constants.QUERY_TRANSLATOR_CLASSNAME(), null);

		this.queryEvaluatorClassName = this.readString(
				Constants.DATASOURCE_READER_CLASSNAME(), null);

		this.queryResultWriterClassName = this.readString(
				Constants.QUERY_RESULT_WRITER_CLASSNAME(), null);

		this.literalRemoveStrangeChars = this.readBoolean(
				Constants.REMOVE_STRANGE_CHARS_FROM_LITERAL(), true);
		logger.debug("Remove Strange Chars From Literal Column = " + this.literalRemoveStrangeChars);

		this.encodeUnsafeChars = this.readBoolean(Constants.ENCODE_UNSAFE_CHARS_IN_URI_COLUMN(), true);
		logger.debug("Encode Unsafe Chars From URI Column = " + this.encodeUnsafeChars);

		this.encodeReservedChars = this.readBoolean(Constants.ENCODE_RESERVED_CHARS_IN_URI_COLUMN(), true);
		logger.debug("Encode Reserved Chars From URI Column = " + this.encodeReservedChars);

	}

	public Connection getConn() {
		return conn;
	}

	public String getJenaMode() {
		return jenaMode;
	}

	public String getRdfLanguage() {
		if(this.rdfLanguage == null) {
			this.rdfLanguage = Constants.OUTPUT_FORMAT_NTRIPLE();
		}
		return rdfLanguage;
	}

	public String getDatabaseType() {
		if(this == null || this.databaseType == null) {
			this.databaseType = Constants.DATABASE_MYSQL();
		}
		return databaseType;
	}

	public String getQueryFilePath() {
		return queryFilePath;
	}

	public String getOntologyFilePath() {
		return ontologyFilePath;
	}

	public boolean isSubQueryElimination() {
		return subQueryElimination;
	}

	public boolean isTransJoinSubQueryElimination() {
		return this.transJoinSubQueryElimination;
	}

	public boolean isTransSTGSubQueryElimination() {
		return this.transSTGSubQueryElimination;
	}

	public boolean isLiteralRemoveStrangeChars() {
		return literalRemoveStrangeChars;
	}

	public Connection openConnection() throws SQLException {
		if(this.conn == null) {
			try {
				this.conn = DBUtility.getLocalConnection(
						databaseUser, databaseName, databasePassword, 
						databaseDriver, 
						databaseURL, "R2ORunner");
			} catch (SQLException e) {
				String errorMessage = "Error loading database, error message = " + e.getMessage();
				logger.error(errorMessage);
				//e.printStackTrace();
				throw e;
			}			
		}
		return this.conn;
	}

	public int getDatabaseTimeout() {
		return databaseTimeout;
	}

	public boolean isEncodeUnsafeChars() {
		return encodeUnsafeChars;
	}

	public boolean isEncodeReservedChars() {
		return encodeReservedChars;
	}

	public boolean isSubQueryAsView() {
		return subQueryAsView;
	}

	public void setSubQueryAsView(boolean subQueryAsView) {
		this.subQueryAsView = subQueryAsView;
	}

	private boolean readBoolean(String property, boolean defaultValue) {
		boolean result = defaultValue;

		String propertyString = this.getProperty(property);
		if(propertyString != null) {
			if(propertyString.equalsIgnoreCase("yes") || propertyString.equalsIgnoreCase("true")) {
				result = true;
			} else if(propertyString.equalsIgnoreCase("no") || propertyString.equalsIgnoreCase("false")) {
				result = false;
			}
		}

		return result;
	}

	private int readInteger(String property, int defaultValue) {
		int result = defaultValue;

		String propertyString = this.getProperty(property);
		if(propertyString != null && !propertyString.equals("")) {
			result = Integer.parseInt(propertyString);
		} 

		return result;
	}

	private String readString(String property, String defaultValue) {
		String result = defaultValue;

		String propertyString = this.getProperty(property);
		if(propertyString != null && !propertyString.equals("")) {
			result = propertyString;
		}
		return result;
	}

	public String getDatabaseUser() {
		return databaseUser;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public String getDatabasePassword() {
		return databasePassword;
	}

	public String getDatabaseDriver() {
		return databaseDriver;
	}

	public String getDatabaseURL() {
		return databaseURL;
	}

	public int getNoOfDatabase() {
		return noOfDatabase;
	}

	public String getQueryResultWriterClassName() {
		return queryResultWriterClassName;
	}

	public String getQueryTranslatorClassName() {
		return queryTranslatorClassName;
	}

	public String getQueryEvaluatorClassName() {
		return queryEvaluatorClassName;
	}

	public void setConn(Connection conn) {
		this.conn = conn;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public void setDatabaseType(String databaseType) {
		this.databaseType = databaseType;
	}

	public boolean isReorderSTG() {
		return reorderSTG;
	}

	public void setMappingDocumentFilePath(String mappingDocumentFilePath) {
		this.mappingDocumentFilePath = mappingDocumentFilePath;
	}

	public void setOutputFilePath(String outputFilePath) {
		this.outputFilePath = outputFilePath;
	}

	public void setDatabaseURL(String databaseURL) {
		this.databaseURL = databaseURL;
	}

	public void setDatabaseUser(String databaseUser) {
		this.databaseUser = databaseUser;
	}

	public void setDatabasePassword(String databasePassword) {
		this.databasePassword = databasePassword;
	}

	public void setNoOfDatabase(int noOfDatabase) {
		this.noOfDatabase = noOfDatabase;
	}

	public void setDatabaseDriver(String databaseDriver) {
		this.databaseDriver = databaseDriver;
	}

	public void setQueryFilePath(String queryFilePath) {
		this.queryFilePath = queryFilePath;
	}

	public void setDatabaseTimeout(int databaseTimeout) {
		this.databaseTimeout = databaseTimeout;
	}


}
