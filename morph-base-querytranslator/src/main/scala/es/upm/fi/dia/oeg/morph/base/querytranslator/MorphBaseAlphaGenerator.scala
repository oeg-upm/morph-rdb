package es.upm.fi.dia.oeg.morph.base.querytranslator

import scala.collection.JavaConversions._
import java.util.Collection
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
	
  def logger = Logger.getLogger("MorphBaseAlphaGenerator");
  
//	val databaseType = {
//		if(this.owner == null) {null}
//		else {this.owner.getDatabaseType();}
//	}
	//val databaseType = md.configurationProperties.databaseType;
	
	def calculateAlpha(tp:Triple, cm:MorphBaseClassMapping, predicateURI:String) : MorphAlphaResult;

	def calculateAlpha(tp:Triple, cm:MorphBaseClassMapping , predicateURI:String , pm:MorphBasePropertyMapping ) 
	: MorphAlphaResult;

	def  calculateAlphaPredicateObject(tp:Triple, cm:MorphBaseClassMapping , pm:MorphBasePropertyMapping  
			, logicalTableAlias:String ) : SQLJoinTable;
	
	def calculateAlphaSubject(subject:Node , cm:MorphBaseClassMapping ) : SQLLogicalTable;
	
	def calculateAlphaSTG(triples:Collection[Triple] , cm:MorphBaseClassMapping ) 
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
	
	def calculateAlphaPredicateObjectSTG(tp:Triple , cm:MorphBaseClassMapping , tpPredicateURI:String 
	    , logicalTableAlias:String ) : List[SQLJoinTable];

	def calculateAlphaPredicateObjectSTG2(tp:Triple , cm:MorphBaseClassMapping , tpPredicateURI:String 
	    ,  logicalTableAlias:String) : List[SQLLogicalTable] ;


	def  calculateAlphaPredicateObject2(triple:Triple , cm:MorphBaseClassMapping , pm:MorphBasePropertyMapping
	    , logicalTableAlias:String ) : SQLLogicalTable;



}