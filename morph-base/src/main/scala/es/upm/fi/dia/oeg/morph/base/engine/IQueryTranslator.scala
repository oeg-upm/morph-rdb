package es.upm.fi.dia.oeg.morph.base.engine

import java.sql.{Connection, ResultSet}

import org.apache.jena.query.Query
import es.upm.fi.dia.oeg.morph.base.sql.IQuery
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.{MorphProperties, TranslatedValue}
import org.apache.jena.graph.Node
//import com.hp.hpl.jena.sparql.algebra.Op
import org.apache.jena.sparql.algebra.Op;
//import com.hp.hpl.jena.sparql.algebra.op.OpBGP
import org.apache.jena.sparql.algebra.op.OpBGP;
import Zql.ZUpdate
import Zql.ZInsert
import Zql.ZDelete

trait IQueryTranslator {
  var connection:Connection = null;

  //	var sparqlQuery :Query = null;

  var optimizer:QueryTranslationOptimizer  = null;

  var properties:MorphProperties =null;

  var databaseType:String =null;

  //val unfolder:MorphBaseUnfolder=null;

  var mappingDocument:MorphBaseMappingDocument= null;

  //	def setSPARQLQueryByString(queryString:String );
  //
  //	def setSPARQLQueryByFile(queryFilePath:String );

  //def getTranslationResult():IQuery ;

  def translate(query:Query) :IQuery ;

  def translate(op:Op) :IQuery ;

  def translateFromQueryFile(queryFilePath:String ) : IQuery;

  //IQueryTranslationOptimizer getOptimizer();

  def translateFromString(queryString:String ) : IQuery;

  //String translateResultSet(String columnLabel, String dbValue);

  def generateNode(rs:ResultSet, varName:String, mapXSDDatatype:Map[String, String]
                   , varNameColumnLabels:List[String]):Node;

  def setDatabaseType(dbType:String) = {this.databaseType = dbType}

  def trans(op:Op) : IQuery;

  def translateUpdate(stg:OpBGP) : ZUpdate;

  def translateInsert(stg:OpBGP) : ZInsert;

  def translateDelete(stg:OpBGP) : ZDelete;

  def setConnection(conn:Connection) = {this.connection = conn}

  def setOptimizer(optimizer:QueryTranslationOptimizer) = { this.optimizer = optimizer }
}