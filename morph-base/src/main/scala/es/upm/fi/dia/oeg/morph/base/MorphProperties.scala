package es.upm.fi.dia.oeg.morph.base;

import java.util.Properties;
import java.io.File;
import java.sql.Connection;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Locale;
import org.slf4j.LoggerFactory

class MorphProperties extends java.util.Properties {
  //val logger = LogManager.getLogger(this.getClass);
  val logger = LoggerFactory.getLogger(this.getClass());

  var configurationFileURL:String =null;
  var configurationDirectory:String=null;

  //	var conn:Connection=null;
  var ontologyFilePath:Option[String]=None;
  var mappingDocumentFilePath:String=null ;

  //database
  var noOfDatabase=0;
  var databaseDriver:String =null;
  var databaseURL:String =null;
  var databaseName:String =null;
  var databaseUser:String =null;
  var databasePassword:String =null;
  var databaseTimeout = 0;
  var databaseBooleanTrue = "true";
  var databaseBooleanFalse = "false";

  var outputFilePath:Option[String] = None;
  var queryFilePath:Option[String] = None;
  var rdfLanguage:String=null;
  var jenaMode:String =null;
  var databaseType:String =null;

  //query translator
  //var queryTranslatorClassName:String =null;
  var queryTranslatorFactoryClassName:String =null;
  var queryEvaluatorClassName:String =null;
  var queryResultWriterFactoryClassName:String =null;

  //query optimizer
  var reorderSTG = true;
  var selfJoinElimination = true;
  var subQueryElimination = true;
  var transJoinSubQueryElimination = true;
  var transSTGSubQueryElimination = true;
  var subQueryAsView = false;

  //batch upgrade
  var literalRemoveStrangeChars:Boolean = true;
  var encodeUnsafeChars:Boolean = true;
  var encodeReservedChars:Boolean = false;
  var transformString:Option[String] = None;
  var mapDataTranslationLimits:Map[String,String] = Map.empty;
  var mapDataTranslationOffsets:Map[String,String] = Map.empty;
  var materializationDistinct:Boolean = false;

  //benchmarking
  var benchmarkFilePath:Option[String] = None;

  //uri encoding
  //var uriEncode:Option[String]=None;
  var mapURIEncodingChars:Map[String, String]=Map.empty;
  var uriTransformationOperation:List[String]=Nil;

  //var inputDatePattern:Option[String] = None;
  val mapLocale = Map("GERMAN" -> Locale.GERMAN, "GERMANY" -> Locale.GERMANY)
  var inputDateFormat:DateFormat = null;
  //var outputDateFormat:DateFormat = new SimpleDateFormat("yyyy-MMM-dd", Locale.ENGLISH);
  var outputDateFormat:DateFormat = null;

  def readConfigurationFile(pConfigurationDirectory:String , configurationFile:String) = {
    var absoluteConfigurationFile = configurationFile;
    var configurationDirectory = pConfigurationDirectory;

    if(configurationDirectory != null) {
      if(!configurationDirectory.endsWith(File.separator)) {
        configurationDirectory = configurationDirectory + File.separator;
      }
      absoluteConfigurationFile = configurationDirectory + configurationFile;
    }
    this.configurationFileURL = absoluteConfigurationFile;
    this.configurationDirectory = configurationDirectory;

    logger.info("reading configuration file : " + absoluteConfigurationFile);
    try {
      val fis = new FileInputStream(absoluteConfigurationFile);
      val isr = new InputStreamReader(fis, "UTF-8");
      this.load(isr);



      //this.load(new StringReader(absoluteConfigurationFile));
    } catch {
      case e:FileNotFoundException => {
        val errorMessage = "Configuration file not found: " + absoluteConfigurationFile;
        logger.error(errorMessage);
        e.printStackTrace();
        throw e;
      }
      case e:IOException => {
        val errorMessage = "Error reading configuration file: " + absoluteConfigurationFile;
        logger.error(errorMessage);
        e.printStackTrace();
        throw e;
      }
    }








    this.mappingDocumentFilePath = this.readString(Constants.MAPPINGDOCUMENT_FILE_PATH, null);
    if(this.mappingDocumentFilePath != null) {
      val isNetResourceMapping = GeneralUtility.isNetResource(this.mappingDocumentFilePath);
      if(!isNetResourceMapping && configurationDirectory != null) {
        this.mappingDocumentFilePath = configurationDirectory + mappingDocumentFilePath;
      }
    }

    val queryFilePathPropertyValue = this.getProperty(Constants.QUERYFILE_PROP_NAME);
    if(queryFilePathPropertyValue != null && !queryFilePathPropertyValue.equals("")) {
      this.queryFilePath = Some(queryFilePathPropertyValue);
    }

    val ontologyFilePathPropertyValue = this.getProperty(Constants.ONTOFILE_PROP_NAME);
    if(ontologyFilePathPropertyValue != null && !ontologyFilePathPropertyValue.equals("")) {
      this.ontologyFilePath = Some(ontologyFilePathPropertyValue);
    }

    val outputFilePropertyValue = this.getProperty(Constants.OUTPUTFILE_PROP_NAME);
    this.outputFilePath = if(outputFilePropertyValue != null
      && !outputFilePropertyValue.equals("")) {
      Some(outputFilePropertyValue)
    } else { None }

    val benchmarkFilePropertyValue = this.getProperty(Constants.BENCHMARKFILE_PROP_NAME);
    this.benchmarkFilePath = if(benchmarkFilePropertyValue != null
      && !benchmarkFilePropertyValue.equals("")) {
      Some(benchmarkFilePropertyValue)
    } else { None }

    if(configurationDirectory != null) {
      if(this.outputFilePath.isDefined) {
        this.outputFilePath = Some(configurationDirectory + outputFilePath.get);
      }

      if(this.benchmarkFilePath.isDefined) {
        this.benchmarkFilePath = Some(configurationDirectory + benchmarkFilePath.get);
      }

      if(this.ontologyFilePath.isDefined) {
        val isNetResourceOntology = GeneralUtility.isNetResource(ontologyFilePath.get);
        if(!isNetResourceOntology) {
          this.ontologyFilePath = Some(configurationDirectory + ontologyFilePath.get);
        }
      }

      if(this.queryFilePath.isDefined) {
        val isNetResourceQuery = GeneralUtility.isNetResource(queryFilePath.get);
        if(!isNetResourceQuery) {
          this.queryFilePath = Some(configurationDirectory + queryFilePath.get);
        }
      }
    }

    this.rdfLanguage = this.readString(Constants.OUTPUTFILE_RDF_LANGUAGE
      , Constants.OUTPUT_FORMAT_NTRIPLE);
    if(this.rdfLanguage == null) {
      this.rdfLanguage = Constants.OUTPUT_FORMAT_NTRIPLE;
    }
    logger.debug("rdf language = " + this.rdfLanguage);

    this.jenaMode = this.readString(Constants.JENA_MODE_TYPE
      , Constants.JENA_MODE_TYPE_MEMORY);
    logger.debug("Jena mode = " + jenaMode);

    this.selfJoinElimination = this.readBoolean(Constants.OPTIMIZE_TB, true);
    logger.debug("Self join elimination = " + this.selfJoinElimination);

    this.reorderSTG = this.readBoolean(Constants.REORDER_STG, true);
    logger.debug("Reorder STG = " + this.reorderSTG);

    this.subQueryElimination = this.readBoolean(Constants.SUBQUERY_ELIMINATION, true);
    logger.debug("Subquery elimination = " + this.subQueryElimination);

    this.transJoinSubQueryElimination = this.readBoolean(
      Constants.TRANSJOIN_SUBQUERY_ELIMINATION, true);
    logger.debug("Trans join subquery elimination = " + this.transJoinSubQueryElimination);

    this.transSTGSubQueryElimination = this.readBoolean(
      Constants.TRANSSTG_SUBQUERY_ELIMINATION, true);
    logger.debug("Trans stg subquery elimination = " + this.transSTGSubQueryElimination);

    this.subQueryAsView = this.readBoolean(Constants.SUBQUERY_AS_VIEW, false);
    logger.debug("Subquery as view = " + this.subQueryAsView);

    this.queryTranslatorFactoryClassName = this.readString(
      Constants.QUERY_TRANSLATOR_FACTORY_CLASSNAME, null);

    this.queryEvaluatorClassName = this.readString(
      Constants.DATASOURCE_READER_CLASSNAME, null);

    this.queryResultWriterFactoryClassName = this.readString(
      Constants.QUERY_RESULT_WRITER_FACTORY_CLASSNAME, null);

    this.literalRemoveStrangeChars = this.readBoolean(
      Constants.REMOVE_STRANGE_CHARS_FROM_LITERAL, true);
    logger.debug("Remove Strange Chars From Literal Column = " + this.literalRemoveStrangeChars);

    this.encodeUnsafeChars = this.readBoolean(Constants.ENCODE_UNSAFE_CHARS_IN_URI_COLUMN, true);
    logger.debug("Encode Unsafe Chars From URI Column = " + this.encodeUnsafeChars);

    this.encodeReservedChars = this.readBoolean(
      Constants.ENCODE_RESERVED_CHARS_IN_URI_COLUMN, false);
    logger.debug("Encode Reserved Chars From URI Column = " + this.encodeReservedChars);

    this.transformString = this.readString(MorphProperties.TRANSFORM_STRING_PROPERTY, None);
    logger.debug("String transformation = " + this.transformString);


    //		val uriEncodeString = this.readString(MorphProperties.URI_ENCODE_PROPERTY, None);
    //		if(uriEncodeString.isDefined) {
    //			val mapURIEncodings = uriEncodeString.get.split(",,");
    //			val mapEncodingChars = mapURIEncodings.map(x => {
    //				val mapEncodingChar = x.substring(1, x.length()-1).split("->");
    //				(mapEncodingChar(0).substring(1, mapEncodingChar(0).length()-1) -> mapEncodingChar(1).substring(1, mapEncodingChar(1).length()-1))
    //			} )
    //			this.mapURIEncodingChars = mapEncodingChars.toMap;
    //			logger.info("this.mapURIEncodingChars = " + this.mapURIEncodingChars);
    //		}
    val uriEncodingPropertyValue = this.readMapStringString(MorphProperties.URI_ENCODE_PROPERTY, Map.empty);
    this.mapURIEncodingChars = uriEncodingPropertyValue;

    this.uriTransformationOperation = this.readListString(MorphProperties.URI_TRANSFORM_PROPERTY
      , Nil, ",") ;

    this.mapDataTranslationLimits = this.readMapStringString(MorphProperties.DATATRANSLATION_LIMIT, Map.empty);
    this.mapDataTranslationOffsets = this.readMapStringString(MorphProperties.DATATRANSLATION_OFFSET, Map.empty);



    val inputDatePatternPropertyValue = this.getProperty(Constants.INPUT_DATE_PATTERN_PROP_NAME);
    val inputDateFormatPattern = if(inputDatePatternPropertyValue != null && !inputDatePatternPropertyValue.equals("")) {
      inputDatePatternPropertyValue;
    } else { "yyyy-MM-dd"; }
    val inputDateLocalePropertyValue = this.getProperty(Constants.INPUT_DATE_LOCALE_PROP_NAME);
    val inputDateLocale = if(inputDateLocalePropertyValue == null) {
      Locale.ENGLISH
    } else { mapLocale.getOrElse(inputDateLocalePropertyValue, Locale.ENGLISH) }
    this.inputDateFormat = new SimpleDateFormat(inputDateFormatPattern, inputDateLocale);

    val outputDatePatternPropertyValue = this.getProperty(Constants.OUTPUT_DATE_PATTERN_PROP_NAME);
    val outputDateFormatPattern = if(outputDatePatternPropertyValue != null && !outputDatePatternPropertyValue.equals("")) {
      outputDatePatternPropertyValue;
    } else { "yyyy-MM-dd"; }
    val outputDateLocalePropertyValue = this.getProperty(Constants.OUTPUT_DATE_LOCALE_PROP_NAME);
    val outputDateLocale = if(outputDateLocalePropertyValue == null) {
      Locale.ENGLISH
    } else { mapLocale.getOrElse(outputDateLocalePropertyValue, Locale.ENGLISH) }
    this.outputDateFormat = new SimpleDateFormat(outputDateFormatPattern, outputDateLocale);


    this.materializationDistinct = this.readBoolean(Constants.MATERIALIZATION_DISTINCT, false);
    logger.debug("Use DISTINCT in materialization process = " + this.materializationDistinct);

    this.databaseBooleanTrue = this.readString(Constants.DATABASE_BOOLEAN_TRUE, "true");
    this.databaseBooleanFalse = this.readString(Constants.DATABASE_BOOLEAN_FALSE, "false");
  }


  def readMapStringString(property:String , defaultValue:Map[String,String]) : Map[String,String] = {
    val propertyString = this.readString(property, None);
    if(propertyString.isDefined) {
      val propertyStringSplited = propertyString.get.split(",,");
      val result = propertyStringSplited.map(x => {
        val resultElement = x.substring(1, x.length()-1).split("->");
        val resultKey =  resultElement(0).substring(1, resultElement(0).length()-1);
        val resultValue = resultElement(1).substring(1, resultElement(1).length()-1)
        val resultAux = (resultKey -> resultValue);
        resultAux;
      } )
      result.toMap;
    } else {
      defaultValue;
    }

  }

  def readBoolean(property:String , defaultValue:Boolean ) : Boolean = {
    val propertyString = this.getProperty(property);
    val result = if(propertyString != null) {
      if(propertyString.equalsIgnoreCase("yes") || propertyString.equalsIgnoreCase("true")) {
        true;
      } else if(propertyString.equalsIgnoreCase("no") || propertyString.equalsIgnoreCase("false")) {
        false;
      } else {
        defaultValue
      }
    } else {
      defaultValue
    }

    result;
  }

  def readInteger(property:String , defaultValue:Int) : Int = {

    val propertyString = this.getProperty(property);
    val result = if(propertyString != null && !propertyString.equals("")) {
      Integer.parseInt(propertyString.trim());
    } else {
      defaultValue
    }

    result;
  }

  def readString(property:String , defaultValue:String ) : String  = {
    val propertyString = this.getProperty(property);
    val result = if(propertyString != null && !propertyString.equals("")) {
      propertyString;
    }
    else { defaultValue }
    return result;
  }

  def readString(property:String , defaultValue:Option[String]) : Option[String] = {
    val propertyString = this.getProperty(property);
    val result = if(propertyString != null && !propertyString.equals("")) {
      Some(propertyString);
    }
    else { defaultValue }
    result;
  }

  def readListString(property:String , defaultValue:List[String], separator:String) : List[String] = {
    val propertyString = this.getProperty(property);
    val result = if(propertyString != null && !propertyString.equals("")) {
      propertyString.split(separator).toList
    }
    else { defaultValue }
    result;
  }


  def setQueryFilePath(queryFilePath:String) = {
    this.queryFilePath = if(queryFilePath == null || queryFilePath.equals("")) {
      None
    } else {Some(queryFilePath)}
  }

  def setOutputFilePath(outputPath:String) = {
    this.outputFilePath = if(outputPath == null || outputPath.equals("")) {
      None
    } else {Some(outputPath)}
  }

  //	def getDatabaseName = this.databaseName;
  //	def getDatabaseType = this.databaseType;

}

object MorphProperties {
  //val logger = LogManager.getLogger(this.getClass);
  val logger = LoggerFactory.getLogger(this.getClass());

  val TRANSFORM_STRING_PROPERTY = "transform.string";

  val URI_ENCODE_PROPERTY = "uri.encode";
  val URI_TRANSFORM_PROPERTY = "uri.transform";

  val DATATRANSLATION_LIMIT="datatranslation.limit";
  val DATATRANSLATION_OFFSET="datatranslation.offset";



  def apply(pConfigurationDirectory:String , configurationFile:String) : MorphProperties = {
    val properties = new MorphProperties();
    properties.readConfigurationFile(pConfigurationDirectory, configurationFile);
    properties
  }
}
