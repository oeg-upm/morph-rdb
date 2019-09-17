package es.upm.fi.dia.oeg.morph.base.querytranslator

import scala.collection.JavaConversions._
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple
import org.apache.jena.vocabulary.RDF
import es.upm.fi.dia.oeg.morph.base.model.MorphBasePropertyMapping
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.sql.SQLJoinTable
import es.upm.fi.dia.oeg.morph.base.sql.SQLLogicalTable
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import org.slf4j.LoggerFactory

abstract class MorphBaseAlphaGenerator(md:MorphBaseMappingDocument,unfolder:MorphBaseUnfolder)
//(val owner:IQueryTranslator) 
{
	var owner:MorphBaseQueryTranslator = null;

	val logger = LoggerFactory.getLogger(this.getClass());

	//	val databaseType = {
	//		if(this.owner == null) {null}
	//		else {this.owner.getDatabaseType();}
	//	}
	//val databaseType = md.configurationProperties.databaseType;

	def calculateAlpha(tp:Triple, cm:MorphBaseClassMapping, predicateURI:String) : MorphAlphaResult;

	def calculateAlpha(tp:Triple, cm:MorphBaseClassMapping , predicateURI:String , pm:MorphBasePropertyMapping )
	: MorphAlphaResult;

	def  calculateAlphaPredicateObject(tp:Triple, cm:MorphBaseClassMapping , pm:MorphBasePropertyMapping
																		 , alphaSubject:SQLLogicalTable ) : Option[MorphAlphaResultPredicateObject];

	def  calculateAlphaPredicateObject(tp:Triple, cm:MorphBaseClassMapping , tpPredicateURI:String
																		 , alphaSubject:SQLLogicalTable ) : Option[MorphAlphaResultPredicateObject] = {
		//val alphaSubjectAlias = alphaSubject.getAlias();

		val isRDFTypeStatement = RDF.`type`.getURI().equals(tpPredicateURI);
		if(isRDFTypeStatement && tp.getObject.isURI) {
			None
		} else {
			val pms = cm.getPropertyMappings(tpPredicateURI);
			if(pms != null && !pms.isEmpty) {
				val pm = pms.iterator.next();
				this.calculateAlphaPredicateObject(tp, cm, pm, alphaSubject);
			} else {
				None
			}
		}
	}

	def calculateAlphaSubject(subject:Node , cm:MorphBaseClassMapping ) : SQLLogicalTable;

	def calculateAlphaSTG(stg:Iterable[Triple] , cm:MorphBaseClassMapping )
	: Iterable[MorphAlphaResultUnion] = {
		//var alphaResultUnionList : List[MorphAlphaResultUnion]  = Nil;

		val firstTriple = stg.iterator.next();
		val tpSubject = firstTriple.getSubject();
		val alphaSubject = this.calculateAlphaSubject(tpSubject, cm);
		val logicalTableAlias = alphaSubject.getAlias();

		val alphaResultUnionList = this.calculateAlphaPredicateObjectSTG(stg, cm, alphaSubject);
		return alphaResultUnionList;
	}

	/*	def calculateAlphaPredicateObjectSTG(tp:Triple , cm:MorphBaseClassMapping , tpPredicateURI:String
        , logicalTableAlias:String ) : List[(SQLJoinTable, String)];*/

	def isProcessableTriplePattern(tp:Triple, triples:Iterable[Triple], mappedClassURIs:Iterable[String]) = {
		val tpPredicate = tp.getPredicate();
		val tpObject = tp.getObject();

		if(tpPredicate.isURI() && tpObject.isURI()) {
			val tpPredicateURI = tpPredicate.getURI();
			val tpObjectURI = tpObject.getURI();

			if(RDF.`type`.getURI().equals(tpPredicateURI) && mappedClassURIs.contains(tpObjectURI) && triples.size > 1) {
				false;
			} else { true; }
		} else {true;}
	}

	def calculateAlphaPredicateObjectSTG(stg:Iterable[Triple] , cm:MorphBaseClassMapping
																			 , alphaSubject:SQLLogicalTable) : Iterable[MorphAlphaResultUnion] = {
		//val alphaSubjectAlias = alphaSubject.getAlias();

		val alphaPredicateObjectSTG = stg.map(tp => {
			val tpPredicate = tp.getPredicate();

			val pms = if (tpPredicate.isURI) {
				cm.getPropertyMappings(tpPredicate.getURI);
			} else if (tpPredicate.isVariable) {
				cm.getPropertyMappings();
			} else {
				val errorMessage = "Predicate has to be either an URI or a variable";
				logger.error(errorMessage);
				Nil;
			}

			val processableTriplePattern = if(tpPredicate.isURI()) {
				val tpPredicateURI = tpPredicate.getURI();
				val mappedClassURIs = cm.getMappedClassURIs();
				this.isProcessableTriplePattern(tp, stg, mappedClassURIs);
			} else {true}

			if(!processableTriplePattern) { None }
			else {
				val alphaTP = new MorphAlphaResultUnion();
				if(pms == null || pms.size == 0) {
					val alphaResult = new MorphAlphaResult(alphaSubject, Nil);
					alphaTP.add(alphaResult);
				} else {
					for(pm <- pms) {
						val tpPredicateURI = pm.getMappedPredicateName(0);
						val alphaPredicateObjectAux : Option[MorphAlphaResultPredicateObject] = calculateAlphaPredicateObject(
							tp, cm, tpPredicateURI, alphaSubject);
						val alphaPredicateObjects:List[MorphAlphaResultPredicateObject] = if(alphaPredicateObjectAux.isEmpty) { Nil }
						else {alphaPredicateObjectAux.toList};

						val alphaResult = new MorphAlphaResult(alphaSubject, alphaPredicateObjects);
						alphaTP.add(alphaResult);
					}
				}

				Some(alphaTP)
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