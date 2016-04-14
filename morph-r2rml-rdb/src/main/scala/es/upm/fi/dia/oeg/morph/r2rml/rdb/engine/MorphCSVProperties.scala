package es.upm.fi.dia.oeg.morph.r2rml.rdb.engine

import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.{Constants, GeneralUtility, MorphProperties}

/**
  * Created by freddy on 14/04/16.
  */
class MorphCSVProperties extends MorphRDBProperties {
  var csvFiles:Option[List[String]]=None;

  override def readConfigurationFile(pConfigurationDirectory:String , configurationFile:String) = {
    super.readConfigurationFile(pConfigurationDirectory, configurationFile);

    //READING cvs.file.path property
    val csvFilePathPropertyValue = this.getProperty(Constants.CSV_FILE_PATH);
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

      this.csvFiles = Some(listOfCSVFiles);
    }
  }
}

object MorphCSVProperties {
	val logger = Logger.getLogger(this.getClass());

	def apply(pConfigurationDirectory:String , configurationFile:String) : MorphCSVProperties = {
			val properties = new MorphCSVProperties();
			properties.readConfigurationFile(pConfigurationDirectory, configurationFile);
			properties
	}
}