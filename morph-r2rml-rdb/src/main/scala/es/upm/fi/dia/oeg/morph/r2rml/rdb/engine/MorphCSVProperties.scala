package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import es.upm.fi.dia.oeg.morph.base.{Constants, GeneralUtility, MorphProperties}
import org.slf4j.LoggerFactory

/**
  * Created by freddy on 14/04/16.
  */
class MorphCSVProperties extends MorphRDBProperties {
  var csvFiles:List[String]=Nil;
  var fieldSeparator:Option[String] = None;

	this.setNoOfDatabase(1);
	this.setDatabaseUser("sa");
	this.setDatabasePassword("");
	this.setDatabaseName("morphcsv");
	this.setDatabaseURL("jdbc:h2:mem:morphcsv");
	this.setDatabaseDriver("org.h2.Driver");
	this.setDatabaseType("CSV");

		
  override def readConfigurationFile(pConfigurationDirectory:String , configurationFile:String) = {
    super.readConfigurationFile(pConfigurationDirectory, configurationFile);

    //READING cvs.file.path property
    val csvFilePathPropertyValue = this.getProperty(Constants.CSV_FILE_PATH);
    this.setCSVFile(csvFilePathPropertyValue);

    this.fieldSeparator = this.readString(Constants.CSV_FIELD_SEPARATOR, None);
    logger.info(s"CSV field separator = ${this.fieldSeparator}")
  }
  
  def setCSVFile(csvFilePathPropertyValue:String) : Unit = {
    if(csvFilePathPropertyValue != null && !csvFilePathPropertyValue.equals("")) {
      val csvFilePathPropertyValueSplited:List[String] = csvFilePathPropertyValue.split(",").map(_.trim).toList;
      val listOfCSVFiles:List[String] = csvFilePathPropertyValueSplited.map(csvFile => {
        val isNetResourceMapping = GeneralUtility.isNetResource(csvFile);
        if(!isNetResourceMapping && configurationDirectory != null) {
          configurationDirectory + csvFile;
        } else {
          csvFile
        }
      });

      this.csvFiles = listOfCSVFiles;
    }    
  }
  
  def addCSVFile(csvFile:String) : Unit = {
    if(this.csvFiles == null) {
      this.csvFiles = Nil;
    }
    this.csvFiles = csvFile :: this.csvFiles;
  }
  
  def setFieldSeparator(fieldSeparator:String) = {
    this.fieldSeparator = Some(fieldSeparator);
  }
  
}

object MorphCSVProperties {
  val logger = LoggerFactory.getLogger(this.getClass());

	def apply(pConfigurationDirectory:String , configurationFile:String) : MorphCSVProperties = {
			val properties = new MorphCSVProperties();
			properties.readConfigurationFile(pConfigurationDirectory, configurationFile);
			properties
	}
}