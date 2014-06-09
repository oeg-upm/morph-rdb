package es.upm.fi.dia.oeg.morph.base.engine

import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import Zql.ZUtils
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.sql.SQLQuery
import es.upm.fi.dia.oeg.morph.base.sql.IQuery
import es.upm.fi.dia.oeg.morph.base.MorphProperties

abstract class MorphBaseUnfolder(md:MorphBaseMappingDocument, properties:MorphProperties  ) {
	Constants.MAP_ZSQL_CUSTOM_FUNCTIONS.foreach(f => { ZUtils.addCustomFunction(f._1, f._2); } )
//	ZUtils.addCustomFunction("concat", 2);
//	ZUtils.addCustomFunction("substring", 3);
//	ZUtils.addCustomFunction("convert", 2);
//	ZUtils.addCustomFunction("coalesce", 2);
//	ZUtils.addCustomFunction("abs", 1);
//	ZUtils.addCustomFunction("lower", 1);
//	ZUtils.addCustomFunction("REPLACE", 3);
//	ZUtils.addCustomFunction("TRIM", 1);
	
	var dbType = Constants.DATABASE_MYSQL;

	def unfoldConceptMapping(cm:MorphBaseClassMapping ) : IQuery;
	
	def unfoldConceptMapping(cm:MorphBaseClassMapping , subjectURI:String ) :IQuery ;
	
	def unfoldSubject(cm:MorphBaseClassMapping ) : IQuery ;
	
	def unfoldMappingDocument():Iterable[IQuery] ;


}