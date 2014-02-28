package es.upm.fi.dia.oeg.morph.base.querytranslator

import scala.collection.JavaConversions._
import java.util.Collection
import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.vocabulary.RDF
import es.upm.fi.dia.oeg.obdi.core.exception.QueryTranslationException
import es.upm.fi.dia.oeg.obdi.core.model.AbstractPropertyMapping
import es.upm.fi.dia.oeg.obdi.core.sql.SQLJoinTable
import es.upm.fi.dia.oeg.obdi.core.sql.SQLLogicalTable
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping
import es.upm.fi.dia.oeg.obdi.core.engine.IQueryTranslator
import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument
import es.upm.fi.dia.oeg.obdi.core.engine.AbstractUnfolder

abstract class MorphBaseAlphaGenerator(md:AbstractMappingDocument,unfolder:AbstractUnfolder)
//(val owner:IQueryTranslator) 
{
	var owner:MorphBaseQueryTranslator = null;
	
  def logger = Logger.getLogger("MorphBaseAlphaGenerator");
  
//	val databaseType = {
//		if(this.owner == null) {null}
//		else {this.owner.getDatabaseType();}
//	}
	val databaseType = md.getConfigurationProperties().databaseType;
	
	def calculateAlpha(tp:Triple, cm:AbstractConceptMapping, predicateURI:String) : MorphAlphaResult;

	def calculateAlpha(tp:Triple, cm:AbstractConceptMapping , predicateURI:String , pm:AbstractPropertyMapping ) 
	: MorphAlphaResult;

	def  calculateAlphaPredicateObject(tp:Triple, cm:AbstractConceptMapping , pm:AbstractPropertyMapping  
			, logicalTableAlias:String ) : SQLJoinTable;
	
	def calculateAlphaSubject(subject:Node , cm:AbstractConceptMapping ) : SQLLogicalTable;
	
	def calculateAlphaSTG(triples:Collection[Triple] , cm:AbstractConceptMapping ) 
	: java.util.List[MorphAlphaResultUnion] = {
		var alphaResultUnionList : List[MorphAlphaResultUnion]  = Nil;
		
		val firstTriple = triples.iterator().next();
		val tpSubject = firstTriple.getSubject();
		val alphaSubject = this.calculateAlphaSubject(tpSubject, cm);
		val logicalTableAlias = alphaSubject.getAlias();
		
		for(tp <- triples) {
			val tpPredicate = tp.getPredicate();
			var alphaPredicateObjects : List[SQLJoinTable] = Nil;
			var alphaPredicateObjects2 : List[SQLLogicalTable] = Nil;
			if(tpPredicate.isURI()) {
				val tpPredicateURI = tpPredicate.getURI();

				val mappedClassURIs = cm.getMappedClassURIs();
				val processableTriplePattern = {
					if(tp.getObject().isURI()) {
						val objectURI = tp.getObject().getURI();
						if(RDF.`type`.getURI().equals(tpPredicateURI) && mappedClassURIs.contains(objectURI)) {
							false;
						} else {
						  true;
						}
					} else {
					  true;
					}				  
				}

				
				if(processableTriplePattern) {
					val alphaPredicateObjectAux = calculateAlphaPredicateObjectSTG(
							tp, cm, tpPredicateURI,logicalTableAlias);
					if(alphaPredicateObjectAux != null) {
						alphaPredicateObjects = alphaPredicateObjects ::: alphaPredicateObjectAux.toList;	
					}

					val alphaPredicateObjectAux2 = calculateAlphaPredicateObjectSTG2(
							tp, cm, tpPredicateURI,logicalTableAlias);
					if(alphaPredicateObjectAux2 != null) {
						alphaPredicateObjects2 = alphaPredicateObjects2 ::: alphaPredicateObjectAux2.toList;	
					}

					val alphaResult = new MorphAlphaResult(alphaSubject
							, alphaPredicateObjects, tpPredicateURI);
					
					val alphaTP = new MorphAlphaResultUnion(alphaResult);
					alphaResultUnionList = alphaResultUnionList ::: List(alphaTP);					
				}
			} else if(tpPredicate.isVariable()){
				val pms = cm.getPropertyMappings();
				val alphaTP = new MorphAlphaResultUnion();
				for(pm <- pms) {
					val tpPredicateURI = pm.getMappedPredicateName(0);
					val alphaPredicateObjectAux = calculateAlphaPredicateObjectSTG(
							tp, cm, tpPredicateURI,logicalTableAlias);
					if(alphaPredicateObjectAux != null) {
						alphaPredicateObjects = alphaPredicateObjects ::: alphaPredicateObjectAux;	
					}
					
					val alphaPredicateObjectAux2 = calculateAlphaPredicateObjectSTG2(
							tp, cm, tpPredicateURI,logicalTableAlias);					
					if(alphaPredicateObjectAux2 != null) {
						alphaPredicateObjects2 = alphaPredicateObjects2 ::: alphaPredicateObjectAux2.toList;	
					}

					val alphaResult = new MorphAlphaResult(alphaSubject
							, alphaPredicateObjects, tpPredicateURI);
					
					alphaTP.add(alphaResult);
				}
				
				if(alphaTP != null) {
					alphaResultUnionList = alphaResultUnionList ::: List(alphaTP);	
				}
				
			} else {
				val errorMessage = "Predicate has to be either an URI or a variable";
				logger.error(errorMessage);
				
			}
		}

		return alphaResultUnionList;
	}
	
	def calculateAlphaPredicateObjectSTG(tp:Triple , cm:AbstractConceptMapping , tpPredicateURI:String 
	    , logicalTableAlias:String ) : List[SQLJoinTable];

	def calculateAlphaPredicateObjectSTG2(tp:Triple , cm:AbstractConceptMapping , tpPredicateURI:String 
	    ,  logicalTableAlias:String) : List[SQLLogicalTable] ;


	def  calculateAlphaPredicateObject2(triple:Triple , cm:AbstractConceptMapping , pm:AbstractPropertyMapping
	    , logicalTableAlias:String ) : SQLLogicalTable;



}