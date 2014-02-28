package es.upm.fi.dia.oeg.morph.base.querytranslator

import es.upm.fi.dia.oeg.obdi.core.engine.IQueryTranslator
import org.apache.log4j.Logger
import scala.collection.JavaConversions._
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping
import Zql.ZConstant
import Zql.ZSelectItem
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.vocabulary.RDF
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLSelectItem
import es.upm.fi.dia.oeg.obdi.core.engine.IQueryTranslator
import es.upm.fi.dia.oeg.obdi.core.exception.QueryTranslationException
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping
import es.upm.fi.dia.oeg.obdi.core.model.AbstractPropertyMapping
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument
import es.upm.fi.dia.oeg.obdi.core.engine.AbstractUnfolder

abstract class MorphBaseBetaGenerator(md:AbstractMappingDocument, unfolder:AbstractUnfolder) {
  
	val logger = Logger.getLogger("MorphBaseBetaGenerator");
	val alphaGenerator:MorphBaseAlphaGenerator=null;
	var owner:MorphBaseQueryTranslator=null;
	
//	val databaseType = {
//		if(this.owner == null) {null}
//		else {this.owner.getDatabaseType();}
//	}
	val dbType = md.getConfigurationProperties().databaseType;
	
	def  calculateBeta(tp:Triple , pos:Constants.MorphPOS.Value, cm:AbstractConceptMapping , predicateURI:String 
			, alphaResult:MorphAlphaResult ) : java.util.List[ZSelectItem] = {
		val result:List[ZSelectItem] = {
			if(pos == Constants.MorphPOS.sub) {
				this.calculateBetaSubject(tp, cm, alphaResult).toList;
			} else if(pos == Constants.MorphPOS.pre) {
				List(this.calculateBetaPredicate(predicateURI));
			} else if(pos == Constants.MorphPOS.obj) {
				val predicateIsRDFSType = RDF.`type`.getURI().equals(predicateURI);
				if(predicateIsRDFSType) {
					val className = new ZConstant(cm.getConceptName(), ZConstant.STRING);
					val selectItem = MorphSQLSelectItem.apply(className);
					//selectItem.setExpression(className);
					List(selectItem);
				} else {
					this.calculateBetaObject(tp, cm, predicateURI, alphaResult).toList;	
				}
			} else {
				throw new QueryTranslationException("invalid Pos value in beta!");
			}
		}
		
		logger.debug("beta = " + result);
		return result;
	}

	def calculateBetaObject(triple:Triple, cm:AbstractConceptMapping , predicateURI:String , alphaResult:MorphAlphaResult )
	: java.util.List[ZSelectItem] = {
		val betaObjects:List[ZSelectItem]  = {
			val pms = cm.getPropertyMappings(predicateURI);
			if(pms == null || pms.isEmpty()) {
				val errorMessage = "Undefined mappings for : " + predicateURI + " for class " + cm.getConceptName();
				logger.debug(errorMessage);
				Nil;
			} else if (pms.size() > 1) {
				val errorMessage = "Multiple property mappings defined, result may be wrong!";
				logger.debug(errorMessage);
				throw new QueryTranslationException(errorMessage);			
			} else {//if(pms.size() == 1)
				val pm = pms.iterator().next();
				this.calculateBetaObject(triple, cm, predicateURI, alphaResult, pm).toList;
			}		  
		}

		return betaObjects;		
	}

	def  calculateBetaObject(triple:Triple, cm:AbstractConceptMapping , predicateURI:String 
	    , alphaResult:MorphAlphaResult, pm:AbstractPropertyMapping ) : java.util.List[ZSelectItem] ;

	def  calculateBetaPredicate(predicateURI:String ) : ZSelectItem = {
		val predicateURIConstant = new ZConstant(predicateURI, ZConstant.STRING);
		val selectItem = MorphSQLSelectItem.apply(predicateURIConstant);
		selectItem;
	}
	
	def calculateBetaSubject(tp:Triple , cm:AbstractConceptMapping , alphaResult:MorphAlphaResult ) : java.util.List[ZSelectItem];

}