package es.upm.fi.dia.oeg.obdi.core.engine;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;

import es.upm.fi.dia.oeg.morph.base.ConfigurationProperties;
import es.upm.fi.dia.oeg.morph.base.Constants;
import es.upm.fi.dia.oeg.morph.base.DBUtility;
import es.upm.fi.dia.oeg.newrqr.RewriterWrapper;
import es.upm.fi.dia.oeg.obdi.core.materializer.AbstractMaterializer;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument;
import es.upm.fi.dia.oeg.obdi.core.sql.IQuery;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLQuery;

public abstract class AbstractRunner {
	private static Logger logger = Logger.getLogger(AbstractRunner.class);
	public ConfigurationProperties configurationProperties;
	protected Connection conn;
	protected AbstractMappingDocument mappingDocument;
	//protected AbstractParser parser;

	//protected Query sparqQuery = null;
	protected AbstractUnfolder unfolder = null;
	protected AbstractDataTranslator dataTranslator;
	private String queryTranslatorFactoryClassName = null;
	protected IQueryTranslator queryTranslator;
	protected DefaultResultProcessor resultProcessor;
	//	private Collection<IQuery> sqlQueries;
	private String queryResultWriterClassName = null;
	private AbstractQueryResultWriter queryResultWriter = null;
	private String dataSourceReaderClassName = null;
	private AbstractDataSourceReader dataSourceReader = null;
	private Object queryResultWriterOutput = null;

	public AbstractRunner() throws Exception {

	}

	public void loadConfigurationProperties(ConfigurationProperties configurationProperties) throws Exception {
		this.configurationProperties = configurationProperties;

		//BUILDING CONNECTION AND DATA SOURCE READER
		this.buildDataSourceReader();
		
		//BUILDING MAPPING DOCUMENT
		String mappingDocumentFile = 
				this.configurationProperties.mappingDocumentFilePath();
		if(mappingDocumentFile != null) {
			this.readMappingDocumentFile(mappingDocumentFile);
		}

		//BUILDING UNFOLDER
		this.unfolder = this.createUnfolder();
		
		//BUILDING DATA TRANSLATOR
		this.createDataTranslator(this.configurationProperties);

		//BUILDING QUERY TRANSLATOR
		this.buildQueryTranslator();

		//BUILDING QUERY RESULT WRITER
		this.buildQueryResultWriter();

		//BUILDING RESULT PROCESSOR
		this.resultProcessor = new DefaultResultProcessor(dataSourceReader, queryResultWriter);				

	}

	public void loadConfigurationfile(String configurationDirectory, String configurationFile) 
			throws Exception {
		ConfigurationProperties configurationProperties = ConfigurationProperties.apply(configurationDirectory, configurationFile);
		this.loadConfigurationProperties(configurationProperties);
	}

	public void buildQueryTranslator() throws Exception {
		this.queryTranslatorFactoryClassName = 
				this.configurationProperties.queryTranslatorFactoryClassName();
		
		if(this.mappingDocument == null) {
			String mappingDocumentFilePath = this.getMappingDocumentPath();
			if(mappingDocumentFilePath == null) {
				throw new Exception("Mapping document is not set yet!");
			}
			this.readMappingDocumentFile(mappingDocumentFilePath);
		}
		
		final String queryTranslatorFactoryClassName;
		if(this.queryTranslatorFactoryClassName == null || this.queryTranslatorFactoryClassName.equals("")) {
			queryTranslatorFactoryClassName = Constants.QUERY_TRANSLATOR_FACTORY_CLASSNAME_DEFAULT();
		} else {
			queryTranslatorFactoryClassName = this.queryTranslatorFactoryClassName; 
		}

//		this.queryTranslator = (IQueryTranslator) Class.forName(queryTranslatorClassName).newInstance();
		IQueryTranslatorFactory queryTranslatorFactory = (IQueryTranslatorFactory) 
				Class.forName(queryTranslatorFactoryClassName).newInstance(); 
		this.queryTranslator = queryTranslatorFactory.createQueryTranslator(this.mappingDocument, this.conn, this.unfolder);
				
		if(configurationProperties != null) {
			this.queryTranslator.setConfigurationProperties(configurationProperties);
			String databaseType = configurationProperties.databaseType();
			if(databaseType != null && !databaseType.equals("")) {
				this.queryTranslator.setDatabaseType(databaseType);
			}			
		}




		//query translation optimizer
		IQueryTranslationOptimizer queryTranslationOptimizer = this.buildQueryTranslationOptimizer();
		boolean eliminateSelfJoin = this.isSelfJoinElimination();
		queryTranslationOptimizer.setSelfJoinElimination(eliminateSelfJoin);
		boolean eliminateSubQuery = this.isSubQueryElimination();
		queryTranslationOptimizer.setSubQueryElimination(eliminateSubQuery);
		boolean transJoinEliminateSubQuery = this.isTransJoinSubQueryElimination();
		queryTranslationOptimizer.setTransJoinSubQueryElimination(transJoinEliminateSubQuery);
		boolean transSTGEliminateSubQuery = this.isTransSTGSubQueryElimination();
		queryTranslationOptimizer.setTransSTGSubQueryElimination(transSTGEliminateSubQuery);
		boolean subQueryAsView = this.isSubQueryAsView();
		queryTranslationOptimizer.setSubQueryAsView(subQueryAsView);
		this.queryTranslator.setOptimizer(queryTranslationOptimizer);
		logger.debug("query translator = " + this.queryTranslator);
		
		//sparql query
		String queryFilePath = this.configurationProperties.queryFilePath();
		this.queryTranslator.setSPARQLQueryByFile(queryFilePath);
	}
	
	private void buildDataSourceReader() throws Exception {
		this.dataSourceReaderClassName = 
				this.configurationProperties.queryEvaluatorClassName();
		
		final String dataSourceReaderClassName;
		
		if(this.dataSourceReaderClassName == null || this.dataSourceReaderClassName.equals("")) {
			dataSourceReaderClassName = Constants.QUERY_EVALUATOR_CLASSNAME_DEFAULT();
		} else {
			dataSourceReaderClassName = this.dataSourceReaderClassName;
		}
		
		this.dataSourceReader = (AbstractDataSourceReader)
				Class.forName(dataSourceReaderClassName).newInstance();
		if(dataSourceReader instanceof RDBReader) {
			//database connection
			if(this.configurationProperties != null) {
				if(this.configurationProperties.noOfDatabase() == 1) {
					try { conn = this.getConnection(); } 
					catch(Exception e) { e.printStackTrace(); }				
				}
				((RDBReader) dataSourceReader).setConnection(conn);
				int timeout = this.configurationProperties.databaseTimeout();
				((RDBReader) dataSourceReader).setTimeout(timeout);
			}
		}

		logger.debug("query evaluator = " + this.dataSourceReader);
	}

	private void buildQueryResultWriter() throws Exception {
		this.queryResultWriterClassName = 
				this.configurationProperties.queryResultWriterClassName();
		
		final String queryResultWriterClassName;
		if(this.queryResultWriterClassName == null || this.queryResultWriterClassName.equals("")) {
			queryResultWriterClassName = Constants.QUERY_RESULT_WRITER_CLASSNAME_DEFAULT(); 
		} else {
			queryResultWriterClassName = this.queryResultWriterClassName;
		}
				
		this.queryResultWriter = (AbstractQueryResultWriter) 
				Class.forName(queryResultWriterClassName).newInstance();

		if(this.queryTranslator == null) {
			throw new Exception("Query Translator is not set yet!");
		}
		this.queryResultWriter.setQueryTranslator(this.queryTranslator);

//		if(this.sparqQuery != null) {
//			this.queryResultWriter.setSparqQuery(sparqQuery);
//		}

		//if(queryResultWriter instanceof XMLQueryResultWriter && this.queryResultWriterOutput == null) {
		if(this.queryResultWriterOutput == null) {
			//set output file
			String outputFileName = null;
			if(this.configurationProperties != null) {
				outputFileName = this.configurationProperties.outputFilePath();
			}
			if(outputFileName == null) {
				outputFileName = Constants.QUERY_RESULT_XMLWRITER_OUTPUT_DEFAULT(); 
			}
			this.queryResultWriterOutput = outputFileName;
		}
		queryResultWriter.setOutput(this.queryResultWriterOutput);

		logger.debug("query result writer = " + this.queryResultWriter);
	}

	protected IQueryTranslationOptimizer buildQueryTranslationOptimizer() {
		final String queryTranslatorClassName = Constants.QUERY_OPTIMIZER_CLASSNAME_DEFAULT();;
		
		try {
			return (IQueryTranslationOptimizer) Class.forName(queryTranslatorClassName).newInstance();
		} catch (Exception e) {
			String errorMessage = "error while building query optimizer instance:" + e.getMessage();
			logger.warn(errorMessage);
		}
		return null;
	}



	protected abstract void createDataTranslator(
			ConfigurationProperties configurationProperties);

	protected abstract AbstractUnfolder createUnfolder();

	public ConfigurationProperties getConfigurationProperties() {
		if(this.configurationProperties == null) {
			this.configurationProperties = new ConfigurationProperties();
		}
		return this.configurationProperties;
	}

	public ConfigurationProperties getConfigurationProperties2() {
		return this.configurationProperties;
	}

	public Connection getConnection() throws SQLException {
		if(this.configurationProperties.noOfDatabase() > 0 && 
				this.conn == null) {
			String databaseUser = this.configurationProperties.databaseUser();
			String databaseName = this.configurationProperties.databaseName();
			String databasePassword = this.configurationProperties.databasePassword();
			String databaseDriver = this.configurationProperties.databaseDriver();
			String databaseURL = this.configurationProperties.databaseURL();
			this.conn = DBUtility.getLocalConnection(
					databaseUser, databaseName, databasePassword, 
					databaseDriver, 
					databaseURL, "Runner");
		}

		return this.conn;
	}

	public AbstractDataTranslator getDataTranslator() {
		return dataTranslator;
	}

	public AbstractMappingDocument getMappingDocument() {
		return mappingDocument;
	}

	public String getMappingDocumentPath() {
		return this.configurationProperties.mappingDocumentFilePath();
	}

	public String getOutputFilePath() {
		return this.configurationProperties.outputFilePath();
	}

	public AbstractQueryResultWriter getQueryResultWriter() {
		return this.queryResultWriter;
	}

	public IQueryTranslator getQueryTranslator() {
		return this.queryTranslator;
	}

	public Query getSparqQuery() {
		Query result = null;

		if(this.queryTranslator != null) {
			result = this.queryTranslator.getSPARQLQuery();
		} 

		return result;
	}

	//	public Collection<IQuery> getSqlQueries() {
	//		return sqlQueries;
	//	}


	protected boolean isSelfJoinElimination() {
		boolean result = true;
		if(this.configurationProperties != null) {
			result = this.configurationProperties.selfJoinElimination();
		}
		return result;
	}

	protected boolean isSubQueryAsView() {
		boolean result = false;
		if(this.configurationProperties != null) {
			result = this.configurationProperties.subQueryAsView();
		}
		return result;
	}


	protected boolean isSubQueryElimination() {
		boolean result = true;
		if(this.configurationProperties != null) {
			result = this.configurationProperties.subQueryElimination();
		}
		return result;
	}


	//	private void setDataTranslator(AbstractDataTranslator dataTranslator) {
	//		this.dataTranslator = dataTranslator;
	//	}

	//	public void setParser(AbstractParser parser) {
	//		this.parser = parser;
	//	}

	protected boolean isTransJoinSubQueryElimination() {
		boolean result = false;
		if(this.configurationProperties != null) {
			result = this.configurationProperties.transJoinSubQueryElimination();
		}
		return result;
	}

	protected boolean isTransSTGSubQueryElimination() {
		boolean result = false;
		if(this.configurationProperties != null) {
			result = this.configurationProperties.transSTGSubQueryElimination();
		}
		return result;
	}

	protected ConfigurationProperties loadConfigurationFile(
			String mappingDirectory, String configurationFile) 
					throws Exception {
		logger.debug("Active Directory = " + mappingDirectory);
		logger.debug("Loading configuration file : " + configurationFile);

		try {
			ConfigurationProperties configurationProperties = ConfigurationProperties.apply(mappingDirectory, configurationFile);
			return configurationProperties;
		} catch(Exception e) {
			logger.error("Error while loding properties file : " + configurationFile);
			throw e;
		}
	}

	private void preMaterializeProcess(String outputFileName) throws Exception {
		//PREPARING OUTPUT FILE
		//OutputStream fileOut = new FileOutputStream (outputFileName);
		//Writer out = new OutputStreamWriter (fileOut, "UTF-8");
		String rdfLanguage = this.configurationProperties.rdfLanguage();
		String jenaMode = configurationProperties.jenaMode();
		AbstractMaterializer materializer = AbstractMaterializer.create(
				rdfLanguage, outputFileName, jenaMode);
		Map<String, String> mappingDocumentPrefixMap = this.mappingDocument.getMappingDocumentPrefixMap(); 
		if(mappingDocumentPrefixMap != null) {
			materializer.setModelPrefixMap(mappingDocumentPrefixMap);
		}
		this.dataTranslator.setMaterializer(materializer);
	}
	
	private void postMaterialize() {
		//		if(rdfLanguage.equalsIgnoreCase(R2OConstants.OUTPUT_FORMAT_RDFXML)) {
		//			if(model == null) {
		//				logger.warn("Model was empty!");
		//			} else {
		//				ModelWriter.writeModelStream(model, configurationProperties.getOutputFilePath(), configurationProperties.getRdfLanguage());
		//				model.close();				
		//			}
		//		}
		
		//cleaning up
		try {
			this.dataTranslator.materializer.postMaterialize();
			//out.flush(); out.close();
			//fileOut.flush(); fileOut.close();
			DBUtility.closeConnection(this.conn, this.getClass().getName());
		} catch(Exception e) {
			e.printStackTrace();
		} finally {

		}
	}
	
	private void materializeMappingDocuments(String outputFileName, AbstractMappingDocument md) 
			throws Exception {
		long start = System.currentTimeMillis();
		
		//PREMATERIALIZE PROCESS
		this.preMaterializeProcess(outputFileName);

		//MATERIALIZING MODEL
		long startGeneratingModel = System.currentTimeMillis();
//		this.dataTranslator.translateData(md);
		Collection<AbstractConceptMapping> cms = md.getConceptMappings();
		//this.dataTranslator.translateData(cms);
		for(AbstractConceptMapping cm:cms) {
			SQLQuery sqlQuery = this.unfolder.unfoldConceptMapping(cm);
			//this.dataTranslator.generateSubjects(cm, sqlQuery);
			this.dataTranslator.generateRDFTriples(cm, sqlQuery.toString());
			
		}
		this.dataTranslator.materializer.materialize();

		//POSTMATERIALIZE PROCESS
		this.postMaterialize();

		long endGeneratingModel = System.currentTimeMillis();
		long durationGeneratingModel = (endGeneratingModel-startGeneratingModel) / 1000;
		logger.info("Materializing Mapping Document time was "+(durationGeneratingModel)+" s.");
	}


	public abstract void readMappingDocumentFile(String mappingDocumentFile) throws Exception;

	public void readSPARQLFile(String sparqQueryFileURL) {
		if(this.queryTranslator != null) {
			Query sparqQuery = QueryFactory.read(sparqQueryFileURL);
			this.queryTranslator.setSPARQLQuery(sparqQuery);
		}
	}

	//	public abstract String getQueryTranslatorClassName();

	
	public void materializeSubjects(
			String classURI, String outputFileName) throws Exception {
		//Map<AbstractConceptMapping, Collection<String>> result = new HashMap<AbstractConceptMapping, Collection<String>>();
		long startGeneratingModel = System.currentTimeMillis();
		
		//PREMATERIALIZE PROCESS
		this.preMaterializeProcess(outputFileName);

		//MATERIALIZING MODEL
		
		Collection<AbstractConceptMapping> cms = 
				this.mappingDocument.getConceptMappingsByConceptName(classURI);
		for(AbstractConceptMapping cm:cms) {
			SQLQuery sqlQuery = this.unfolder.unfoldConceptMapping(cm);
			this.dataTranslator.generateSubjects(cm, sqlQuery.toString());
		}
		this.dataTranslator.materializer.materialize();

		//POSTMATERIALIZE PROCESS
		this.postMaterialize();

		long endGeneratingModel = System.currentTimeMillis();
		long durationGeneratingModel = (endGeneratingModel-startGeneratingModel) / 1000;
		logger.info("Materializing Subjects time was "+(durationGeneratingModel)+" s.");
		
		//return result;
	}

	public void materializeInstanceDetails(
			String subjectURI, String classURI, String outputFileName) throws Exception {
		//Map<AbstractConceptMapping, Collection<String>> result = new HashMap<AbstractConceptMapping, Collection<String>>();
		long startGeneratingModel = System.currentTimeMillis();
		
		//PREMATERIALIZE PROCESS
		this.preMaterializeProcess(outputFileName);

		//MATERIALIZING MODEL
		Collection<AbstractConceptMapping> cms = 
				this.mappingDocument.getConceptMappingsByConceptName(classURI);
		for(AbstractConceptMapping cm:cms) {
			SQLQuery sqlQuery = this.unfolder.unfoldConceptMapping(cm, subjectURI);
			if(sqlQuery != null) {
				this.dataTranslator.generateRDFTriples(cm, sqlQuery.toString());	
			}
		}
		this.dataTranslator.materializer.materialize();

		//POSTMATERIALIZE PROCESS
		this.postMaterialize();

		long endGeneratingModel = System.currentTimeMillis();
		long durationGeneratingModel = (endGeneratingModel-startGeneratingModel) / 1000;
		logger.info("Materializing Subjects time was "+(durationGeneratingModel)+" s.");
		
		//return result;
	}

	public String run()
			throws Exception {
		String status = null;

		//mapping document
		if(this.mappingDocument == null) {
			String mappingDocumentFile = 
					this.configurationProperties.mappingDocumentFilePath();
			if(mappingDocumentFile != null) {
				this.readMappingDocumentFile(mappingDocumentFile);
			}			
		}

		Query sparqlQuery = this.queryTranslator.getSPARQLQuery();
		if(sparqlQuery == null) {
			//set output file
			String outputFileName = configurationProperties.outputFilePath();
			this.materializeMappingDocuments(outputFileName, mappingDocument);
		} else {
			logger.debug("sparql query = " + sparqlQuery);

			//query translator
			if(this.queryTranslator == null) {
				if(this.queryTranslatorFactoryClassName == null) {
					this.queryTranslatorFactoryClassName = Constants.QUERY_TRANSLATOR_FACTORY_CLASSNAME_DEFAULT();					
				}
				this.buildQueryTranslator();
			}

			//query result writer
			if(this.queryResultWriter == null) {
				if(this.queryResultWriterClassName == null) {
					this.queryResultWriterClassName = Constants.QUERY_RESULT_WRITER_CLASSNAME_DEFAULT();					
				}
				this.buildQueryResultWriter();
			}

			//query evaluator
			if(this.dataSourceReader == null) {
				if(this.dataSourceReaderClassName == null) {
					this.dataSourceReaderClassName = Constants.QUERY_EVALUATOR_CLASSNAME_DEFAULT();
				}
				this.buildDataSourceReader();
			}

			//result processor
			this.resultProcessor = new DefaultResultProcessor(
					dataSourceReader, queryResultWriter);				

			//loading ontology file
			String ontologyFilePath = null;
			if(this.configurationProperties != null) {
				ontologyFilePath = this.configurationProperties.ontologyFilePath();
			}


			//rewrite the SPARQL query if necessary
			List<Query> queries = new ArrayList<Query>();
			if(ontologyFilePath == null || ontologyFilePath.equals("")) {
				queries.add(sparqlQuery);
			} else {
				//rewrite the query based on the mappings and ontology
				logger.info("Rewriting query...");
				this.configurationProperties.mappingDocumentFilePath();
				//				Collection <String> mappedOntologyElements = MappingsExtractor.getMappedPredcatesFromR2O(mappingDocumentFile);
				Collection <String> mappedOntologyElements = this.mappingDocument.getMappedConcepts();
				Collection <String> mappedOntologyElements2 = this.mappingDocument.getMappedProperties();
				mappedOntologyElements.addAll(mappedOntologyElements2);


				//RewriterWrapper rewritterWapper = new RewriterWrapper(ontologyFilePath, rewritterWrapperMode, mappedOntologyElements);
				//queries = rewritterWapper.rewrite(originalQuery);
				queries = RewriterWrapper.rewrite(sparqlQuery, ontologyFilePath, RewriterWrapper.fullMode, mappedOntologyElements, RewriterWrapper.globalMatchMode);

				logger.debug("No of rewriting query result = " + queries.size());
				logger.debug("queries = " + queries);
			}			


			//translate sparql queries into sql queries
			Collection<IQuery> sqlQueries = 
					this.translateSPARQLQueriesIntoSQLQueries(queries);

			//translate result
			//if (this.conn != null) {
			//GFT does not need a Connection instance
			this.resultProcessor.translateResult(sqlQueries);	
			//}
		}

		logger.info("**********************DONE****************************");
		return status;

	}

	public void setDataSourceReaderClassName(String queryEvaluatorClassName) {
		this.dataSourceReaderClassName = queryEvaluatorClassName;
	}

	public void setQueryResultWriterClassName(String queryResultWriterClassName) throws Exception {
		this.queryResultWriterClassName = queryResultWriterClassName;
		//this.buildQueryResultWriter();
	}

	public void setQueryResultWriterOutput(Object output) throws Exception {
		this.queryResultWriterOutput = output;
	}

	public void setQueryTranslator(IQueryTranslator queryTranslator) {
		this.queryTranslator = queryTranslator;
	}

	public void setQueryTranslatorFactoryClassName(String queryTranslatorFactoryClassName) throws Exception {
		this.queryTranslatorFactoryClassName = queryTranslatorFactoryClassName;
		//this.buildQueryTranslator();
	}

	public void setResultTranslator(DefaultResultProcessor resultTranslator) {
		this.resultProcessor = resultTranslator;
	}

//	public void setSparqQuery(Query sparqQuery) {
//		if(this.queryTranslator != null) {
//			this.queryTranslator.setSparqlQuery(sparqQuery);
//		}
//	}
//
//	public void setSparqQuery(String sparqQueryString) {
//		if(this.queryTranslator != null) {
//			Query sparqQuery = QueryFactory.create(sparqQueryString);
//			this.queryTranslator.setSparqlQuery(sparqQuery);
//		}
//	}

	private Collection<IQuery> translateSPARQLQueriesIntoSQLQueries(
			Collection<Query> sparqlQueries) throws Exception {
		Collection<IQuery> sqlQueries = new Vector<IQuery>();
		for(Query sparqlQuery : sparqlQueries) {
			logger.debug("SPARQL Query = \n" + sparqlQuery);
			IQuery sqlQuery = this.queryTranslator.translate(sparqlQuery);
			logger.debug("SQL Query = \n" + sqlQuery);
			sqlQueries.add(sqlQuery);
		}

		return sqlQueries;
	}


}
