package es.upm.fi.dia.oeg.morph.base.querytranslator

import Zql.ZSelectItem

class MorphPRSQLResult (val prSQLSubject:List[ZSelectItem]
    , val prSQLPredicates:List[ZSelectItem], val prSQLObjects:List[ZSelectItem]) {
	def toList() = { this.prSQLSubject ::: this.prSQLPredicates ::: this.prSQLObjects}
}