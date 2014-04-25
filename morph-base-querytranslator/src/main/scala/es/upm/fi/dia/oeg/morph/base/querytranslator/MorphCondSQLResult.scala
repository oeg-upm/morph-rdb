package es.upm.fi.dia.oeg.morph.base.querytranslator

import Zql.ZExpression
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLUtility
import es.upm.fi.dia.oeg.morph.base.Constants

class MorphCondSQLResult(val condSQLSubject:Iterable[ZExpression], val condSQLPredicateObjects:Iterable[ZExpression]  ) {
	def toExpression() = {
	  MorphSQLUtility.combineExpresions(this.toList, Constants.SQL_LOGICAL_OPERATOR_AND);
	}
	
	def toList() = {
	  condSQLSubject.toList ::: condSQLPredicateObjects.toList
	}
}