package es.upm.fi.dia.oeg.morph.base.querytranslator

import scala.collection.JavaConversions._
import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.vocabulary.RDF
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.model.MorphBasePropertyMapping
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.sql.SQLJoinTable
import es.upm.fi.dia.oeg.morph.base.sql.SQLLogicalTable
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder

abstract class MorphBaseAlphaGenerator(md:MorphBaseMappingDocument,unfolder:MorphBaseUnfolder)
//(val owner:IQueryTranslator) 
{
	var owner:MorphBaseQueryTranslator = null;
	
  	val logger = Logger.getLogger(this.getClass());
  
//	val databaseType = {
//		if(this.owner == null) {null}
//		else {this.owner.getDatabaseType();}
//	}
	//val databaseType = md.configurationProperties.databaseType;
	
	def calculateAlpha(tp:Triple, cm:MorphBaseClassMapping, predicateURI:String) : MorphAlphaResult;

	def calculateAlpha(tp:Triple, cm:MorphBaseClassMapping , predicateURI:String , pm:MorphBasePropertyMapping ) 
	: MorphAlphaResult;

	def  calculateAlphaPredicateObject(tp:Triple, cm:MorphBaseClassMapping , pm:MorphBasePropertyMapping  
			, logicalTableAlias:String ) : (SQLJoinTable, String);
	
	def calculateAlphaSubject(subject:Node , cm:MorphBaseClassMapping ) : SQLLogicalTable;

	def isProcessableTriplePattern(tp:Triple, triples:Iterable[Triple], mappedClassURIs:Iterable[String]) = {
		val tpPredicate = tp.getPredicate();
		val tpObject = tp.getObject();

		if(tpPredicate.isURI() && tpObject.isURI()) {
			val tpPredicateURI = tpPredicate.getURI();
			val tpObjectURI = tpObject.getURI();

			if(RDF.`type`.getURI().equals(tpPredicateURI) && mappedClassURIs.contains(tpObjectURI) && triples.size > 1) {
				false;
			} else {
				true;
			}
		} else {
			true;
		}
	}

	def calculateAlphaSTG(stg:Iterable[Triple] , cm:MorphBaseClassMapping )
	: Iterable[MorphAlphaResultUnion] = {
		//var alphaResultUnionList : List[MorphAlphaResultUnion]  = Nil;
		
		val firstTriple = stg.iterator.next();
		val tpSubject = firstTriple.getSubject();
		val alphaSubject = this.calculateAlphaSubject(tpSubject, cm);
		val logicalTableAlias = alphaSubject.getAlias();

		val alphaResultUnionList = this.calculateAlphaPredicateObjectSTG(stg, cm, alphaSubject, logicalTableAlias);
		return alphaResultUnionList;
	}
	
	def calculateAlphaPredicateObjectSTG(tp:Triple , cm:MorphBaseClassMapping , tpPredicateURI:String 
	    , logicalTableAlias:String ) : List[(SQLJoinTable, String)];

	def calculateAlphaPredicateObjectSTG(stg:Iterable[Triple] , cm:MorphBaseClassMapping , alphaSubject:SQLLogicalTable
																			 , logicalTableAlias:String ) : Iterable[MorphAlphaResultUnion] = {

		val alphaPredicateObjectSTG = stg.map(tp => {
			val tpPredicate = tp.getPredicate();
			//val alphaPredicateObjects : List[(SQLJoinTable, String)] = Nil;
			if(tpPredicate.isURI()) {
				val tpPredicateURI = tpPredicate.getURI();
				val mappedClassURIs = cm.getMappedClassURIs();
				val processableTriplePattern = this.isProcessableTriplePattern(tp, stg, mappedClassURIs);

				val alphaPredicateObject = if(processableTriplePattern) {
					val alphaPredicateObjectAux = calculateAlphaPredicateObjectSTG(
						tp, cm, tpPredicateURI,logicalTableAlias);

					val alphaPredicateObjects = if(alphaPredicateObjectAux != null) {
						alphaPredicateObjectAux.toList;
					} else {
						Nil;
					}

					val alphaResult = new MorphAlphaResult(alphaSubject
						, alphaPredicateObjects);

					val alphaTP = new MorphAlphaResultUnion(alphaResult);
					Some(alphaTP);
				} else {
					None;
				}
				alphaPredicateObject;
			} else if(tpPredicate.isVariable()){
				val pms = cm.getPropertyMappings();
				val alphaTP = new MorphAlphaResultUnion();
				for(pm <- pms) {
					val tpPredicateURI = pm.getMappedPredicateName(0);
					val alphaPredicateObjectAux = calculateAlphaPredicateObjectSTG(
						tp, cm, tpPredicateURI,logicalTableAlias);
					val alphaPredicateObjects = if(alphaPredicateObjectAux != null) {
						alphaPredicateObjectAux;
					} else {
						Nil
					}
					val alphaResult = new MorphAlphaResult(alphaSubject
						, alphaPredicateObjects);

					alphaTP.add(alphaResult);
				}

				val alphaPredicateObject = if(alphaTP != null) {
					Some(alphaTP);
				} else {
					None;
				}
				alphaPredicateObject;
			} else {
				val errorMessage = "Predicate has to be either an URI or a variable";
				logger.error(errorMessage);
				None;
			}
		});
		alphaPredicateObjectSTG.flatMap(x => x);

	}

//	def calculateAlphaPredicateObjectSTG2(tp:Triple , cm:MorphBaseClassMapping , tpPredicateURI:String 
//	    ,  logicalTableAlias:String) : List[SQLLogicalTable] ;
//
//
//	def  calculateAlphaPredicateObject2(triple:Triple , cm:MorphBaseClassMapping , pm:MorphBasePropertyMapping
//	    , logicalTableAlias:String ) : SQLLogicalTable;



}