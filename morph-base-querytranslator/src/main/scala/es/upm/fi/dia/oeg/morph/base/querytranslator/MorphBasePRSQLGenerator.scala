package es.upm.fi.dia.oeg.morph.base.querytranslator
import scala.collection.JavaConversions._
import Zql.ZSelectItem
import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.vocabulary.RDF
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLSelectItem
import java.util.Collection
import org.apache.log4j.Logger
import scala.collection.mutable.LinkedHashSet
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder

class MorphBasePRSQLGenerator(md:MorphBaseMappingDocument, unfolder:MorphBaseUnfolder) {

  val logger = Logger.getLogger("MorphBasePRSQLGenerator");
//	val databaseType = {
//		if(this.owner == null) {null}
//		else {this.owner.getDatabaseType();}
//	}
	//val dbType = md.configurationProperties.databaseType;
  val dbType = md.dbMetaData.dbType;
	var mapHashCodeMapping : Map[Integer, Object] = Map.empty

	
	def  genPRSQL(tp:Triple , alphaResult:MorphAlphaResult 
	    , betaGenerator:MorphBaseBetaGenerator, nameGenerator:NameGenerator
	    , cmSubject:MorphBaseClassMapping, predicateURI:String , unboundedPredicate:Boolean) 
	: List[ZSelectItem] = {
		val tpSubject = tp.getSubject();
		val tpPredicate = tp.getPredicate();
		val tpObject = tp.getObject();
		

		var prList : List[ZSelectItem ]= Nil;

		val selectItemsSubjects = this.genPRSQLSubject(tp, alphaResult, betaGenerator, nameGenerator, cmSubject);
		prList = prList ::: selectItemsSubjects.toList;

		if(tpPredicate != tpSubject) {
			//line 22
			if(unboundedPredicate) {
				val selectItemPredicate = this.genPRSQLPredicate(tp, alphaResult, betaGenerator, nameGenerator, predicateURI);
				if(selectItemPredicate != null) {
					prList = prList ::: List(selectItemPredicate);
				}				
			}
		}

		if(tpObject != tpSubject && tpObject != tpPredicate) {
			val columnType = {
				if(tpPredicate.isVariable()) {
					if(Constants.DATABASE_POSTGRESQL.equalsIgnoreCase(dbType)) {
						Constants.POSTGRESQL_COLUMN_TYPE_TEXT;	
					} else if(Constants.DATABASE_MONETDB.equalsIgnoreCase(dbType)) {
						Constants.MONETDB_COLUMN_TYPE_TEXT;
					} else {
						Constants.MONETDB_COLUMN_TYPE_TEXT;
					}
				} else {
				  null
				}
			}
			
			//line 23
			val objectSelectItems = this.genPRSQLObject(tp, alphaResult, betaGenerator, nameGenerator
			    ,cmSubject, predicateURI, columnType);
			prList = prList ::: objectSelectItems.toList;
		}

		logger.debug("genPRSQL = " + prList);
		prList;
	}
	
	def genPRSQLObject(tp:Triple, alphaResult:MorphAlphaResult
	    , betaGenerator:MorphBaseBetaGenerator, nameGenerator:NameGenerator 
	    , cmSubject:MorphBaseClassMapping , predicateURI:String , columnType:String) 
	: List[ZSelectItem] = {
		
		def betaObjSelectItems = betaGenerator.calculateBetaObject(tp, cmSubject, predicateURI, alphaResult);
		val selectItems = for(i <- 0 until betaObjSelectItems.size()) yield {
			val betaObjSelectItem = betaObjSelectItems.get(i);
			val selectItem = MorphSQLSelectItem.apply(betaObjSelectItem, dbType, columnType);
			
			val selectItemAliasAux = nameGenerator.generateName(tp.getObject());
			val selectItemAlias = {
				if(selectItemAliasAux != null) {
					if(betaObjSelectItems.size() > 1) {
						selectItemAliasAux + "_" + i;
					} else {
					  selectItemAliasAux
					}
				} else {
				  selectItemAliasAux
				}
			}
			
			if(selectItemAlias != null) {
			  selectItem.setAlias(selectItemAlias);
			}
			
			selectItem; //line 23
		}
		selectItems.toList;
	}

	def  genPRSQLPredicate(tp:Triple, alphaResult:MorphAlphaResult 
	    , betaGenerator:MorphBaseBetaGenerator, nameGenerator:NameGenerator 
	    , predicateURI:String ) : ZSelectItem = {
			val betaPre = betaGenerator.calculateBetaPredicate(predicateURI);
			val selectItem = MorphSQLSelectItem.apply(betaPre, this.dbType, "text");

			val alias = nameGenerator.generateName(tp.getPredicate());
			selectItem.setAlias(alias);
			return selectItem;

	}
	
	def  genPRSQLSubject(tp:Triple , alphaResult:MorphAlphaResult 
	    , betaGenerator:MorphBaseBetaGenerator, nameGenerator:NameGenerator 
	    , cmSubject:MorphBaseClassMapping ) : List[ZSelectItem] = {
		val tpSubject = tp.getSubject();
		
		val prSubjects = {
			if(!tpSubject.isBlank()) {
				val betaSubSelectItems = betaGenerator.calculateBetaSubject(tp, cmSubject, alphaResult);
				for(i <- 0 until betaSubSelectItems.size()) yield {
					val betaSub = betaSubSelectItems.get(i);
						
					val selectItem = MorphSQLSelectItem.apply(betaSub, dbType);
					val selectItemSubjectAliasAux = nameGenerator.generateName(tpSubject);
					val selectItemSubjectAlias = {
						if(betaSubSelectItems.size() > 1) {
							selectItemSubjectAliasAux + "_" + i;
						} else {
							 selectItemSubjectAliasAux
						}
					}
						
					selectItem.setAlias(selectItemSubjectAlias);
					selectItem;
				}
			} else {
			  Nil
			}		  
		}
		prSubjects.toList
	}
	


	def  genPRSQLSTG(stg:Collection[Triple],alphaResult:MorphAlphaResult 
	    , betaGenerator:MorphBaseBetaGenerator,nameGenerator:NameGenerator 
	    , cmSubject:MorphBaseClassMapping ) : List[ZSelectItem] = {
		
		val firstTriple = stg.iterator.next();
		val selectItemsSubjects = this.genPRSQLSubject(firstTriple, alphaResult, betaGenerator, nameGenerator, cmSubject);

		val tpSubject = firstTriple.getSubject();
		var selectItemsSTGObjects : LinkedHashSet[ZSelectItem] = new LinkedHashSet[ZSelectItem]();
		
		for(tp <- stg)  {
			val tpPredicate = tp.getPredicate();
			if(!tpPredicate.isURI()) {
				val errorMessage = "Only bounded predicate is supported in STG.";
				logger.warn(errorMessage);
			}
			val predicateURI = tpPredicate.getURI();
			
			if(RDF.`type`.getURI().equals(predicateURI)) {
				//do nothing
			} else {
				val tpObject = tp.getObject();
				if(tpPredicate != tpSubject) {
					val selectItemPredicate = this.genPRSQLPredicate(tp, alphaResult
					    , betaGenerator, nameGenerator,predicateURI);
					if(selectItemPredicate != null) {
						//prList.add(selectItemPredicate);	
					}
					
				}
				if(tpObject != tpSubject && tpObject != tpPredicate) {
					val selectItemsObject = this.genPRSQLObject(tp, alphaResult, betaGenerator
					    , nameGenerator, cmSubject, predicateURI, null);
					selectItemsSTGObjects  = selectItemsSTGObjects ++ selectItemsObject;
				} else {
				}				
			}
		}

		val prList = selectItemsSubjects.toList ::: selectItemsSTGObjects.toList;
		prList;
	}
	
	def getMappedMapping(hashCode:Integer ) = {
		this.mapHashCodeMapping.get(hashCode);
	}
	
	def putMappedMapping(key:Integer , value:Object ) {
		this.mapHashCodeMapping += (key -> value);
	}	

}