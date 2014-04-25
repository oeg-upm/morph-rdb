package es.upm.fi.dia.oeg.morph.base.querytranslator

import scala.collection.JavaConversions._
import es.upm.fi.dia.oeg.morph.base.sql.IQuery
import es.upm.fi.dia.oeg.morph.base.sql.SQLQuery
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.sql.SQLFromItem
import es.upm.fi.dia.oeg.morph.base.engine.QueryTranslationOptimizer
import Zql.ZUpdate
import es.upm.fi.dia.oeg.morph.base.Constants
import Zql.ZInsert
import Zql.ZExpression
import Zql.ZConstant
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLConstant
import Zql.ZDelete
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLUtility
import es.upm.fi.dia.oeg.morph.base.sql.SQLUnion

class MorphTransTPResult(val alphaResult:MorphAlphaResult
    , val condSQLResult:MorphCondSQLResult, val prSQLResult:MorphPRSQLResult) {

	val logger = Logger.getLogger(this.getClass());
  
	def toQuery(optimizer:QueryTranslationOptimizer, databaseType:String) : IQuery = {
		val alphaResult = this.alphaResult
		val alphaSubject = alphaResult.alphaSubject;
		val alphaPredicateObjects = alphaResult.alphaPredicateObjects;
		val prSQLResult = this.prSQLResult;
		val prSQL = prSQLResult.toList;
		val condSQLResult = this.condSQLResult;
		val condSQL = { if(condSQLResult != null) { condSQLResult.toExpression; } 
			else { null }
		}
		
		val resultAux = {
			//don't do subquery elimination here! why?
			if(optimizer != null && optimizer.subQueryElimination) {
				try {
					SQLQuery.createQuery(alphaSubject, alphaPredicateObjects
					    , prSQL, condSQL, databaseType);
				} catch {
				  case e:Exception => {
					val errorMessage = "error in eliminating subquery!";
					logger.error(errorMessage);
					null;							    
				  }
				}
			} else { null } 					  
		}


		if(resultAux == null) { //without subquery elimination or error occured during the process
			val resultAux2 = new SQLQuery(alphaSubject);
			if(alphaPredicateObjects != null) {
				for(alphaPredicateObject <- alphaPredicateObjects) {
				  alphaSubject match {
				    case _:SQLFromItem => {
				      resultAux2.addFromItem(alphaPredicateObject);//alpha predicate object}
				    }
				    case _:SQLQuery => {
						val onExpression = alphaPredicateObject.onExpression;
						alphaPredicateObject.onExpression = null;
						resultAux2.addFromItem(alphaPredicateObject);//alpha predicate object
						resultAux2.pushFilterDown(onExpression);							      
				    }
				    case _ => {
				      resultAux2.addFromItem(alphaPredicateObject);//alpha predicate object
				    }
				  }
				}					
			}
			resultAux2.setSelectItems(prSQL);
			resultAux2.setWhere(condSQL);
			resultAux2;
		} else {
		  resultAux
		}	  
	}

	def toUpdate() = {
	  val alphaSubject = this.alphaResult.alphaSubject;
	  val tableName = alphaSubject.print(true); 
	  val zUpdate = new ZUpdate(tableName);
	  
	  val condSQLSubject = this.condSQLResult.condSQLSubject;
	  zUpdate.addWhere(MorphSQLUtility.combineExpresions(condSQLSubject, Constants.SQL_LOGICAL_OPERATOR_AND));
	  
	  val condSQLPredicateObjects = this.condSQLResult.condSQLPredicateObjects;
	  val mapSetValue = this.condSQLResult.condSQLPredicateObjects.flatMap(x => {
	    if(x.getOperator().equals("=")) {
	      Some(x.getOperand(0).toString() -> x.getOperand(1))
	    }
	    else { None }
	  }).toMap
	  mapSetValue.foreach { case(k,v) => { zUpdate.addColumnUpdate(k, v)} } 
	  
	  zUpdate
	}
	
	def toInsert() = {
	  val alphaSubject = this.alphaResult.alphaSubject;
	  val tableName = alphaSubject.print(false); 
	  val zInsert = new ZInsert(tableName);
	  
	  val condSQLs = this.condSQLResult.toList;
	  val condSQLTuples = condSQLs.flatMap(x => {
	    if(x.getOperator().equals("=")) {
	      val key = x.getOperand(0)
	      val column = key match {
	        case zConstant:ZConstant => {
	          val morphConstant = MorphSQLConstant(zConstant);
	          val column = morphConstant.column.toString();
	          column
	        }
	        case _ => key.toString();
	      }
	      val value = x.getOperand(1);
	      Some((column, value))
	    }
	    else { None }
	  }).toList
	  val keys = condSQLTuples.map(tuple => tuple._1);
	  val values = condSQLTuples.map(tuple => tuple._2);
	  
	  val columns = new java.util.Vector[String]();
	  keys.foreach(key => columns.add(key));
	  zInsert.addColumns(columns);
	  
	  val valueSpec = new ZExpression(",")
	  values.foreach(value => valueSpec.addOperand(value));
	  zInsert.addValueSpec(valueSpec);
	  
	  zInsert
	}
	
	def toDelete() = {
	  val alphaSubject = this.alphaResult.alphaSubject;
	  val tableName = alphaSubject.print(false); 
	  val zDelete = new ZDelete(tableName);
	  
	  val condSQLSubject = this.condSQLResult.condSQLSubject;
	  val newExpressions = condSQLSubject.map(x => {
	    if(x.getOperator().equals("=")) {
	      val key = x.getOperand(0)
	      val column = key match {
	        case zConstant:ZConstant => {
	          val morphConstant = MorphSQLConstant(zConstant);
	          val column = morphConstant.column.toString();
	          column
	        }
	        case _ => key.toString();
	      }
	      val value = x.getOperand(1);
	      val newExpression = new ZExpression("=", new ZConstant(column, ZConstant.COLUMNNAME), value);
	      newExpression
	    }
	    else { x }
	  }).toList
	  val newExpression = MorphSQLUtility.combineExpresions(newExpressions, Constants.SQL_LOGICAL_OPERATOR_AND);
	  zDelete.addWhere(newExpression);
	  
	  zDelete
	}
	
}