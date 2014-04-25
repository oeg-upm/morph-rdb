package es.upm.fi.dia.oeg.morph.base.querytranslator

import Zql.ZExpression
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLUtility
import es.upm.fi.dia.oeg.morph.base.Constants

class MorphCondSQLResult(val condSQLSubject:ZExpression, val condSQLPredicateObjects:Iterable[ZExpression]  ) {
	def toExpression() = {
	  val expressionList = List(condSQLSubject) ::: condSQLPredicateObjects.toList;
	  MorphSQLUtility.combineExpresions(expressionList, Constants.SQL_LOGICAL_OPERATOR_AND);
	}
	
	def toList() = {
	  List(condSQLSubject) ::: condSQLPredicateObjects.toList
	}
}