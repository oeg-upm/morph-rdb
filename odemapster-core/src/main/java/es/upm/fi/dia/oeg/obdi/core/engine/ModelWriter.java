package es.upm.fi.dia.oeg.obdi.core.engine;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.rdf.model.Model;

public class ModelWriter {
	private static Logger logger = Logger.getLogger(ModelWriter.class);
	
	public static void writeModelStream(Model model, String outputFilename, String rdfLanguage) throws Exception {
		try {
			if(model != null) {
				logger.debug("Size of model = " + model.size());
				logger.info("Writing model to " + outputFilename + " ......");
				long startWritingModel = System.currentTimeMillis();
				FileOutputStream resultOutputStream = new FileOutputStream(outputFilename);
				model.write(resultOutputStream, rdfLanguage);
				long endWritingModel = System.currentTimeMillis();
				long durationWritingModel = (endWritingModel-startWritingModel) / 1000;
				logger.info("Writing model time was "+(durationWritingModel)+" s.");				
			}
		} catch(FileNotFoundException fnfe) {
			logger.error("File " + outputFilename + " can not be found!");
			throw fnfe;			
		} catch(Exception e) {
			logger.error("Error writing model because " + e.getMessage());
			throw e;
		}
	}

	/*
	public static void writeModelWriter(Model model, String outputFilename, String rdfLanguage) throws Exception {
		try {
			if(model != null) {
				logger.debug("Size of model = " + model.size());
				logger.info("Writing (writer mode) model to " + outputFilename + " ......");
				long startWritingModel = System.currentTimeMillis();
				FileWriter fw = new FileWriter(outputFilename);
				model.write(fw, rdfLanguage);
				long endWritingModel = System.currentTimeMillis();
				long durationWritingModel = (endWritingModel-startWritingModel) / 1000;
				logger.info("Writing model time was "+(durationWritingModel)+" s.");				
			}
		} catch(FileNotFoundException fnfe) {
			logger.error("File " + outputFilename + " can not be found!");
			throw fnfe;			
		} catch(Exception e) {
			logger.error("Error writing model because " + e.getMessage());
			throw e;
		}
	}
	*/


}
