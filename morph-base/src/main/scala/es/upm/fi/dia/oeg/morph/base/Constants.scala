package es.upm.fi.dia.oeg.morph.base

import Zql.ZExpression
import Zql.ZConstant
import com.hp.hpl.jena.rdf.model.ResourceFactory

class Constants {

	
	
}

object Constants {
	val JOINS_TYPE_INNER = "INNER";
	val JOINS_TYPE_LEFT = "LEFT";
	
	val SQL_LOGICAL_OPERATOR_AND = "AND";
	val SQL_LOGICAL_OPERATOR_OR = "OR";
	val SQL_KEYWORD_UNION = "UNION";
	val SQL_KEYWORD_ORDER_BY = "ORDER BY";
	
	val SQL_EXPRESSION_TRUE = new ZExpression(
			"=", new ZConstant("1", ZConstant.NUMBER), new ZConstant("1", ZConstant.NUMBER));
	val SQL_EXPRESSION_FALSE = new ZExpression(
			"=", new ZConstant("1", ZConstant.NUMBER), new ZConstant("0", ZConstant.NUMBER));
	
	val DATABASE_MONETDB = "MonetDB";
	val DATABASE_ORACLE = "Oracle";
	val DATABASE_MYSQL = "MySQL";
	val DATABASE_SQLSERVER = "SQLServer";
	val DATABASE_POSTGRESQL = "PostgreSQL";
	val DATABASE_GFT = "GFT";

	val DATABASE_POSTGRESQL_ENCLOSED_CHARACTER = "\"";
	val DATABASE_MONETDB_ENCLOSED_CHARACTER = "\"";
	val DATABASE_MYSQL_ENCLOSED_CHARACTER = "`";
	val DATABASE_GFT_ENCLOSED_CHARACTER = "'";
	val DATABASE_ENCLOSED_CHARACTERS = List(
	    DATABASE_POSTGRESQL_ENCLOSED_CHARACTER, DATABASE_MONETDB_ENCLOSED_CHARACTER
	    , DATABASE_MYSQL_ENCLOSED_CHARACTER, DATABASE_GFT_ENCLOSED_CHARACTER)
	
	val POSTGRESQL_COLUMN_TYPE_TEXT = "text";
	val POSTGRESQL_COLUMN_TYPE_INTEGER = "integer";
	val MONETDB_COLUMN_TYPE_TEXT = "clob";
	val MONETDB_COLUMN_TYPE_INTEGER = "integer";
	
	val MAP_DATABASE_COLUMN_TYPE_TEXT = Map(
	    DATABASE_POSTGRESQL -> POSTGRESQL_COLUMN_TYPE_TEXT, DATABASE_MONETDB -> MONETDB_COLUMN_TYPE_TEXT);
	
//	MAP_DATABASE_COLUMN_TYPE_TEXT += (DATABASE_POSTGRESQL -> POSTGRESQL_COLUMN_TYPE_TEXT);
//	MAP_DATABASE_COLUMN_TYPE_TEXT += (DATABASE_MONETDB -> MONETDB_COLUMN_TYPE_TEXT);
	
	//jena specific properties
	val JENA_MODE_TYPE = "jena.mode.type";
	val JENA_MODE_TYPE_MEMORY = "memory";
	val JENA_MODE_TYPE_HSQL = "hsql";
	val JENA_MODE_TYPE_TDB = "tdb";
	val JENA_TDB_DIRECTORY = "jena.tdb.dir";
	val JENA_TDB_FILEBASE = "jena.tdb.filebase";

	//rdf format
	val OUTPUT_FORMAT_RDFXML = "RDF/XML";
	val OUTPUT_FORMAT_RDFXML_ABBREV = "RDF/XML-ABBREV";
	val OUTPUT_FORMAT_NTRIPLE = "N-TRIPLE";
	val OUTPUT_FORMAT_NQUAD= "N-QUAD";
	val OUTPUT_FORMAT_TURTLE = "TURTLE";
	val OUTPUT_FORMAT_N3 = "N3";

	
	//database
	val NO_OF_DATABASE_NAME_PROP_NAME = "no_of_database";
	val DATABASE_NAME_PROP_NAME = "database.name";
	val DATABASE_DRIVER_PROP_NAME = "database.driver";
	val DATABASE_URL_PROP_NAME = "database.url";
	val DATABASE_USER_PROP_NAME = "database.user";
	val  DATABASE_PWD_PROP_NAME = "database.pwd";
	val  DATABASE_TYPE_PROP_NAME = "database.type";
	val  DATABASE_TIMEOUT_PROP_NAME = "database.timeout";

	
	val  MAPPINGDOCUMENT_FILE_PATH = "mappingdocument.file.path";
	val  QUERYFILE_PROP_NAME = "query.file.path";
	val  ONTOFILE_PROP_NAME = "onto.file.path";
	val  ONTOURL_PROP_NAME = "onto.url.path";
	val  OUTPUTFILE_PROP_NAME = "output.file.path";
	val  OUTPUTFILE_RDF_LANGUAGE = "output.rdflanguage";
	val  SPLIT_OUTPUT_PER_CONCEPT = "split_output_per_concept";
	
	val  OPTIMIZE_TB = "querytranslator.selfjoinelimination";
	val  REORDER_STG = "querytranslator.reorderstg";
	val  SUBQUERY_ELIMINATION = "querytranslator.subqueryelimination";
	val  TRANSJOIN_SUBQUERY_ELIMINATION = "querytranslator.transjoin.subqueryelimination";
	val  TRANSSTG_SUBQUERY_ELIMINATION = "querytranslator.transstg.subqueryelimination";
	val  SUBQUERY_AS_VIEW = "querytranslator.subqueryasview";
	val  QUERY_TRANSLATOR_CLASSNAME = "querytranslator.class.name";
//	val  QUERY_TRANSLATOR_CLASSNAME_DEFAULT = 
//			"es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.querytranslator.R2RMLQueryTranslator";
	val  QUERY_TRANSLATOR_CLASSNAME_DEFAULT = 
			"es.upm.fi.dia.oeg.morph.rdb.querytranslator.MorphQueryTranslator";
	
	val DATASOURCE_READER_CLASSNAME = "datasourcereader.class.name";
	val QUERY_EVALUATOR_CLASSNAME_DEFAULT = "es.upm.fi.dia.oeg.obdi.core.engine.RDBReader";
	val QUERY_RESULT_WRITER_CLASSNAME = "queryresult.writer.class.name";
	val QUERY_RESULT_WRITER_CLASSNAME_DEFAULT = "es.upm.fi.dia.oeg.obdi.core.engine.XMLWriter";
	val QUERY_RESULT_XMLWRITER_OUTPUT_DEFAULT = "output.rdf.xml";
	val QUERY_OPTIMIZER_CLASSNAME_DEFAULT = "es.upm.fi.dia.oeg.obdi.core.querytranslator.QueryTranslationOptimizer";

	val  REMOVE_STRANGE_CHARS_FROM_LITERAL = "literal.removestrangechars";
	val  ENCODE_UNSAFE_CHARS_IN_URI_COLUMN = "uricolumn.encodeunsafecharacters";
	val  ENCODE_RESERVED_CHARS_IN_URI_COLUMN = "uricolumn.encodereserveccharacters";
	
	//aliases
	val  URI_AS_ALIAS="uri_";
	val  RANGE_TABLE_ALIAS="rt_";
	val  VIEW_ALIAS="v_";
	val  TABLE_ALIAS_PREFIX="t_";

	//prefixes and suffixes
	val  PREFIX_URI = "uri_";
	val  PREFIX_VAR = "var_";
	val  PREFIX_LIT = "lit_";
	val  KEY_SUFFIX = "_key";
	val  PREFIX_SUBJECT_MAPPING = "sm_";
	val  PREFIX_MAPPING_ID = "mappingid_";
	
	//aggregation function
	val  AGGREGATION_FUNCTION_AVG = "AVG";
	val  AGGREGATION_FUNCTION_MAX = "MAX";
	val  AGGREGATION_FUNCTION_MIN = "MIN";
	val  AGGREGATION_FUNCTION_COUNT = "COUNT";
	val  AGGREGATION_FUNCTION_SUM = "SUM";

	val R2RML_NS = "http://www.w3.org/ns/r2rml#";
	
	//TermMap
	val R2RML_SUBJECTMAP_URI = R2RML_NS + "subjectMap";
  	val R2RML_SUBJECTMAP_PROPERTY = ResourceFactory.createProperty(R2RML_SUBJECTMAP_URI);
	val R2RML_CLASS_URI = R2RML_NS + "class";
	val R2RML_CLASS_PROPERTY = ResourceFactory.createProperty(R2RML_CLASS_URI);
	val R2RML_PREDICATEOBJECTMAP_URI = R2RML_NS + "predicateObjectMap";
	val R2RML_PREDICATEOBJECTMAP_PROPERTY = ResourceFactory.createProperty(R2RML_PREDICATEOBJECTMAP_URI);
	val R2RML_OBJECTMAP_URI = R2RML_NS + "objectMap";
	val R2RML_OBJECTMAP_PROPERTY = ResourceFactory.createProperty(R2RML_OBJECTMAP_URI);
	val R2RML_PARENTTRIPLESMAP_URI = R2RML_NS + "parentTriplesMap";
	val R2RML_PARENTTRIPLESMAP_PROPERTY = ResourceFactory.createProperty(R2RML_PARENTTRIPLESMAP_URI);

	//TermType
	val R2RML_TERMTYPE_URI = R2RML_NS + "termType";
	val R2RML_TERMTYPE_PROPERTY = ResourceFactory.createProperty(R2RML_TERMTYPE_URI);
	val R2RML_LITERAL_URI = R2RML_NS + "Literal";
	val R2RML_LITERAL_CLASS = ResourceFactory.createResource(R2RML_LITERAL_URI);
	val R2RML_IRI_URI = R2RML_NS + "IRI";
	val R2RML_IRI_CLASS = ResourceFactory.createResource(R2RML_IRI_URI);
	val R2RML_BLANKNODE_URI = R2RML_NS + "BlankNode";
	val R2RML_TERMTYPE_IRI_URI = R2RML_NS + "IRI";
	val R2RML_TERM_TYPE_IRI_RESOURCE = ResourceFactory.createResource(R2RML_TERMTYPE_IRI_URI);
	val R2RML_DATATYPE_URI = R2RML_NS + "datatype";
	val R2RML_DATATYPE_PROPERTY = ResourceFactory.createProperty(R2RML_DATATYPE_URI);
	//TermMap
	val R2RML_GRAPHMAP_URI = R2RML_NS + "graphMap";
	val R2RML_GRAPHMAP_PROPERTY = ResourceFactory.createProperty(R2RML_GRAPHMAP_URI);
	val R2RML_LANGUAGE_URI = R2RML_NS + "language";
	val R2RML_LANGUAGE_PROPERTY = ResourceFactory.createProperty(R2RML_LANGUAGE_URI);

	//TermMap Types
	val R2RML_CONSTANT_URI = R2RML_NS + "constant";
	val R2RML_CONSTANT_PROPERTY = ResourceFactory.createProperty(R2RML_CONSTANT_URI);
	val R2RML_COLUMN_URI = R2RML_NS + "column";
	val R2RML_COLUMN_PROPERTY = ResourceFactory.createProperty(R2RML_COLUMN_URI);
	val R2RML_TEMPLATE_URI = R2RML_NS + "template";
	val R2RML_TEMPLATE_PROPERTY = ResourceFactory.createProperty(R2RML_TEMPLATE_URI);
	
	//TriplesMap
	val R2RML_LOGICALTABLE_URI = R2RML_NS + "logicalTable";
	val R2RML_LOGICALTABLE_PROPERTY = ResourceFactory.createProperty(R2RML_LOGICALTABLE_URI);
	//TriplesMap
	val R2RML_SUBJECT_URI = R2RML_NS + "subject";
	val R2RML_SUBJECT_PROPERTY = ResourceFactory.createProperty(R2RML_SUBJECT_URI);
	val R2RML_PREDICATE_URI = R2RML_NS + "predicate";
	val R2RML_PREDICATE_PROPERTY = ResourceFactory.createProperty(R2RML_PREDICATE_URI);
	val R2RML_OBJECT_URI = R2RML_NS + "object";
	val R2RML_OBJECT_PROPERTY = ResourceFactory.createProperty(R2RML_OBJECT_URI);
	val R2RML_GRAPH_URI = R2RML_NS + "graph";
	val R2RML_GRAPH_PROPERTY = ResourceFactory.createProperty(R2RML_GRAPH_URI);

	//logical table
	val R2RML_TABLENAME_URI = R2RML_NS + "tableName";
	val R2RML_TABLENAME_PROPERTY = ResourceFactory.createProperty(R2RML_TABLENAME_URI);
	val R2RML_SQLQUERY_URI = R2RML_NS + "sqlQuery";
	val R2RML_SQLQUERY_PROPERTY = ResourceFactory.createProperty(R2RML_SQLQUERY_URI);


	//MappingDocument
	val R2RML_TRIPLESMAP_URI = R2RML_NS + "TriplesMap";
	val R2RML_TRIPLESMAP_CLASS = ResourceFactory.createResource(R2RML_TRIPLESMAP_URI);
	val R2RML_OBJECTMAPCLASS_URI = R2RML_NS + "ObjectMap";
	val R2RML_OBJECTSMAP_CLASS = ResourceFactory.createResource(R2RML_OBJECTMAPCLASS_URI);

	//PredicateObjectMap
	val R2RML_PREDICATEMAP_URI = R2RML_NS + "predicateMap";
	val R2RML_PREDICATEMAP_PROPERTY = ResourceFactory.createProperty(R2RML_PREDICATEMAP_URI);
	val R2RML_REFOBJECTMAP_URI = R2RML_NS + "refObjectMap";
	val R2RML_REFOBJECTMAP_PROPERTY = ResourceFactory.createProperty(R2RML_REFOBJECTMAP_URI);
	val R2RML_JOINCONDITION_URI = R2RML_NS + "joinCondition";
	val R2RML_JOINCONDITION_PROPERTY = ResourceFactory.createProperty(R2RML_JOINCONDITION_URI);
	val R2RML_CHILD_URI = R2RML_NS + "child";
	val R2RML_CHILD_PROPERTY = ResourceFactory.createProperty(R2RML_CHILD_URI);
	val R2RML_PARENT_URI = R2RML_NS + "parent";
	val R2RML_PARENT_PROPERTY = ResourceFactory.createProperty(R2RML_PARENT_URI);
	
	
	
	
	
	
	//graph
	val R2RML_DEFAULT_GRAPH_URI = R2RML_NS + "defaultGraph";
	val R2RML_DEFAULT_GRAPH_CLASS = ResourceFactory.createResource(R2RML_DEFAULT_GRAPH_URI);

	val R2RML_TEMPLATE_PATTERN = "\\{(.+?)\\}";	
 
	def getEnclosedCharacter(dbType:String) : String = {
	    if (Constants.DATABASE_GFT.equalsIgnoreCase(dbType)) {
	      Constants.DATABASE_GFT_ENCLOSED_CHARACTER;
	    } else if (Constants.DATABASE_MONETDB.equalsIgnoreCase(dbType) ) {
	      Constants.DATABASE_MONETDB_ENCLOSED_CHARACTER;
	    } else if(Constants.DATABASE_MYSQL.equalsIgnoreCase(dbType)) {
	      Constants.DATABASE_MYSQL_ENCLOSED_CHARACTER;
	    } else if (Constants.DATABASE_POSTGRESQL.equalsIgnoreCase(dbType)) {
	      Constants.DATABASE_POSTGRESQL_ENCLOSED_CHARACTER;
	    } else {	    
	      ""
	    }	    
	}
}