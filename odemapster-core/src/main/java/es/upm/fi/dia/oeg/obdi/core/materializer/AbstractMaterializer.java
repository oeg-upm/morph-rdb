package es.upm.fi.dia.oeg.obdi.core.materializer;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.tdb.TDBFactory;

import es.upm.fi.dia.oeg.morph.base.Constants;

public abstract class AbstractMaterializer {
	private static Logger logger = Logger.getLogger(AbstractMaterializer.class);
	protected String outputFileName;
	protected Model model;
	protected String rdfLanguage;
	
	public abstract Object createSubject(boolean isBlankNode, String subjectURI);
	public abstract void materializeDataPropertyTriple(String predicateName, Object objectValue, String datatype, String lang, String graph);
	public abstract void materializeObjectPropertyTriple(String predicateName, String rangeURI, boolean isBlankNodeObject, String graph);
	public abstract void materializeRDFTypeTriple(String subjectURI, String conceptName, boolean isBlankNodeSubject, String graph);
	public abstract void materialize() throws IOException;
	
	public static AbstractMaterializer create(
			String rdfLanguage, String outputFileName, String jenaMode) throws IOException {
		
		if(rdfLanguage.equalsIgnoreCase(Constants.OUTPUT_FORMAT_NTRIPLE())) {
			Model model = null;
			model = AbstractMaterializer.createJenaModel(jenaMode);
			return new NTripleMaterializer(outputFileName, model);
		} else if(rdfLanguage.equalsIgnoreCase(Constants.OUTPUT_FORMAT_RDFXML())) {
			Model model = AbstractMaterializer.createJenaModel(jenaMode);
			return new RDFXMLMaterializer(outputFileName, model, rdfLanguage);
		} else if(rdfLanguage.equalsIgnoreCase(Constants.OUTPUT_FORMAT_NQUAD())) {
			Model model = AbstractMaterializer.createJenaModel(jenaMode);
			return new RDFXMLMaterializer(outputFileName, model, rdfLanguage);
		} else {
			Model model = AbstractMaterializer.createJenaModel(jenaMode);
			return new NTripleMaterializer(outputFileName, model);
		}
	}
	
	public void setModelPrefixMap(Map<String, String> prefixMap) {
		this.model.setNsPrefixes(prefixMap);
	}
	
	public static Model createJenaModel(String jenaMode) {
		Model model = null;
		
		if(jenaMode == null) {
			//logger.warn("Unspecified jena mode, memory based will be used!");
			model = AbstractMaterializer.createJenaMemoryModel();
		} else {
			if(jenaMode.equalsIgnoreCase(Constants.JENA_MODE_TYPE_HSQL())) {
				//logger.debug("jena mode = idb hsqldb");
				//model = AbstractMaterializer.createJenaHSQLDBModel();
			} else if(jenaMode.equalsIgnoreCase(Constants.JENA_MODE_TYPE_TDB())) {
				//logger.debug("jena mode = tdb");
				model = AbstractMaterializer.createJenaTDBModel();
			} else if (jenaMode.equalsIgnoreCase(Constants.JENA_MODE_TYPE_MEMORY())){
				//logger.debug("jena mode = memory");
				model = AbstractMaterializer.createJenaMemoryModel();
			} else {
				//logger.warn("invalid mode of jena type, memory mode will be used.");
				model = AbstractMaterializer.createJenaMemoryModel();
			}				
		}		

		return model;
	}
	
	private static Model createJenaMemoryModel() {
		return ModelFactory.createDefaultModel();
	}

//	private static Model createJenaHSQLDBModel() {
//		try {
//			String className = "org.hsqldb.jdbcDriver";       // path of driver class
//			Class.forName(className);                        // Load the Driver
//			String DB_URL =    "jdbc:hsqldb:file:testdb4";   // URL of database 
//			String DB_USER =   "sa";                          // database user id
//			String DB_PASSWD = "";                            // database password
//			String DB =        "HSQL";                        // database type
//
//			// Create database connection
//			IDBConnection conn = new DBConnection ( DB_URL, DB_USER, DB_PASSWD, DB );
//			ModelMaker maker = ModelFactory.createModelRDBMaker(conn) ;
//
//			// create or open the default model
//			Model model = maker.createDefaultModel();
//
//			// Close the database connection
//			conn.close();
//
//			return model;
//		} catch(Exception e) {
//			e.printStackTrace();
//			return null;
//		}
//	}

	private static Model createJenaTDBModel() {
		String jenaDatabaseName = System.currentTimeMillis() + ""; 
		String tdbDatabaseFolder = "tdb-database";
		File folder = new File(tdbDatabaseFolder);
		if(!folder.exists()) {
			folder.mkdir();
		}

		String tdbFileBase = tdbDatabaseFolder + "/" + jenaDatabaseName;
		logger.info("TDB filebase = " + tdbFileBase);
		return TDBFactory.createModel(tdbFileBase) ;

	}	
}
