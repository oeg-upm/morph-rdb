package es.upm.fi.dia.oeg.morph.base.querytranslator

import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import org.apache.log4j.Logger
import Zql.ZConstant
import Zql.ZExp
import Zql.ZExpression
import Zql.ZSelectItem
import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.vocabulary.RDF
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.MorphTriple
import es.upm.fi.dia.oeg.morph.base.SPARQLUtility
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLConstant
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLUtility
import es.upm.fi.dia.oeg.morph.base.model.MorphBasePropertyMapping
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder

abstract class MorphBaseCondSQLGenerator(md:MorphBaseMappingDocument, unfolder:MorphBaseUnfolder) {
	val logger = Logger.getLogger(this.getClass().getName());
	//val dbType = md.configurationProperties.databaseType;
		
	def  genCondSQL(tp:Triple, alphaResult:MorphAlphaResult
	    , betaGenerator:MorphBaseBetaGenerator, cm:MorphBaseClassMapping
	    ,  predicateURI:String) : MorphCondSQLResult =  {

		val condSQLSubject = this.genCondSQLSubject(tp, alphaResult, betaGenerator, cm);

		val condSQLPredicateObject = {
			try {
				this.genCondSQLPredicateObject(tp, alphaResult, betaGenerator, cm, predicateURI);			
			} catch {
			  case e:Exception => {
			    logger.error(e.getMessage());
			  }
			  null
			}		  
		}

		val condSQLTP = List(condSQLSubject, condSQLPredicateObject);
		val condSQL = MorphSQLUtility.combineExpresions(condSQLTP, Constants.SQL_LOGICAL_OPERATOR_AND);

		logger.debug("genCondSQL = " + condSQL);
		new MorphCondSQLResult(condSQL);
	}



	def generateIsNotNullExpression(betaObjectExpression:ZExp) : ZExpression = {
		val exp = new ZExpression("IS NOT NULL");
		exp.addOperand(betaObjectExpression);
		return exp;
	}

	def genCondSQLPredicateObject(tp:Triple, alphaResult:MorphAlphaResult 
	    , betaGenerator:MorphBaseBetaGenerator, cm:MorphBaseClassMapping
	    , predicateURI:String) : ZExpression = {
		
		//var exps : Set[ZExpression] = Set.empty;
		
		//val mapColumnMetaData = cm.getLogicalTable().getColumnsMetaData();
		val tableMetaData = cm.getLogicalTable().tableMetaData;
		
		val isRDFTypeStatement = RDF.`type`.getURI().equals(predicateURI);

		val pms = cm.getPropertyMappings(predicateURI);
		val pmsSize = pms.size();
		
		val result : ZExpression = {
			if(pms == null || pmsSize == 0) {
				if(!isRDFTypeStatement) {
					val errorMessage = "No mappings found for predicate : " + predicateURI;
					logger.error(errorMessage);
					throw new Exception(errorMessage);				
				} else {
				  null
				}
			} else if(pms.size() > 1) {
				val errorMessage = "Multiple mappings are not permitted for predicate " + predicateURI;
				logger.error(errorMessage);
				throw new Exception(errorMessage);
				null
			} else {//if(pms.size() == 1)
				var exps : Set[ZExpression] = Set.empty;
			  
				val subject = tp.getSubject();
				val predicate = tp.getPredicate();
				val tpObject= tp.getObject();
				
				val pm = pms.iterator.next();
				val result1 = this.genCondSQLPredicateObject(tp, alphaResult, betaGenerator, cm, pm);
				if(result1 != null) {
					exps = exps ++ Set(result1);	
				}
	
				val betaSubjectSelectItems = betaGenerator.calculateBetaSubject(tp, cm, alphaResult);
				val betaSubjectExpressions = betaSubjectSelectItems.map(betaSubjectSelectItem => {
					betaSubjectSelectItem.getExpression();
				})
				  
				val betaPredicateExpression = betaGenerator.calculateBetaPredicate(predicateURI).getExpression();
				
				val betaObjectSelectItems = 
						betaGenerator.calculateBetaObject(tp, cm, predicateURI, alphaResult);
				val betaObjectExpressions= betaObjectSelectItems.map(betaObjectSelectItem => {
						val betaObjectExp = {
							if(betaObjectSelectItem.isExpression()) {
								betaObjectSelectItem.getExpression();
							} else {
								new ZConstant(betaObjectSelectItem.toString()
										, ZConstant.COLUMNNAME);
							}
						}
		
						betaObjectExp;
				  })
				
				
	
				if(!predicate.isVariable()) { //line 08
						new ZExpression("="
								, betaPredicateExpression
								, new ZConstant(predicate.toString(), ZConstant.STRING));				
				}
	
				if(!tpObject.isVariable()) { //line 09
					val exp = {
						if(tpObject.isURI()) {
							val objConstant = new ZConstant(tpObject.getURI(), ZConstant.STRING);
							val expAux = betaObjectExpressions.map(betaObjectExpression => {
							  new ZExpression("=", betaObjectExpression, objConstant);
							})
							expAux
						} else if(tpObject.isLiteral()) {
							val literalValue = tpObject.getLiteralValue();
							literalValue match {
							  case literalValueString:String => {
								val objConstant = new ZConstant(literalValue.toString(), ZConstant.STRING);
								val expAux = betaObjectExpressions.map(betaObjectExpression => {
								  new ZExpression("=", betaObjectExpression, objConstant);
								})
								expAux
							  }
							  case literalValueString:java.lang.Double => {
								val objConstant = new ZConstant(literalValue.toString(), ZConstant.NUMBER);
								val expAux = betaObjectExpressions.map(betaObjectExpression => {
								  new ZExpression("=", betaObjectExpression, objConstant);
								})
								expAux						    
							  }
							  case _ => {
							    betaObjectExpressions.map(betaObjectExpression => {
									val objConstant = new ZConstant(literalValue.toString(), ZConstant.STRING);
									new ZExpression("=", betaObjectExpression, objConstant);						      
							    })
							  }
							}
						}
					}
	
					if(exp != null) {
						//result = new ZExpression("AND", result, exp);
					}
	
				} else { //object.isVariable() // improvement by Freddy
					if(!SPARQLUtility.isBlankNode(tpObject)) {
						val isSingleTripleFromTripleBlock = {
							tp match {
							  case etp:MorphTriple => {
								if(etp.isSingleTripleFromTripleBlock) {
									true;
								} else {
								  false
								}						    
							  }
							  case _ => {false}
							}
						}
	
						//for dealing with unbound() function, we should remove this part
						if(!isSingleTripleFromTripleBlock) {
							
							for(betaObjectExpression <- betaObjectExpressions) {
								betaObjectExpression match {
								  case betaObjectZConstant:ZConstant => {
									val betaColumnConstant = MorphSQLConstant.apply(betaObjectZConstant);
									val betaColumn = betaColumnConstant.column;
	
									val cmd = {
										if(tableMetaData.isDefined ) {
										  if(tableMetaData.get.getColumnMetaData(betaColumn).isDefined) {
										    tableMetaData.get.getColumnMetaData(betaColumn).get
										  } else {
										    null
										  }
										} else {
										  null
										}
									}
									
									if(cmd == null || cmd.isNullable) {
										val exp = this.generateIsNotNullExpression(betaObjectExpression);
										if(exp != null) {
											exps = exps ++ Set(exp);
										}							
									}							    
								  }
								}
							}
						}					
					}
				}
				
				if(subject == predicate) { //line 10
					if(betaSubjectExpressions.size() == 1) {
						for(betaSubjectExpression <- betaSubjectExpressions) {
							val exp = new ZExpression("=", betaSubjectExpression, betaPredicateExpression);
							exps = exps ++ Set(exp);					
						}
					}
				}
	
				if(subject == tpObject) { //line 11
					if(betaSubjectExpressions.size() == betaObjectExpressions.size()) {
						for(i <- 0 until betaSubjectExpressions.size()) {
							val betaSubjectExpression = betaSubjectExpressions.get(i);
							val betaObjectExpression = betaObjectExpressions.get(i);
							val exp = new ZExpression("=", betaSubjectExpression, betaObjectExpression);
							exps = exps ++ Set(exp);
						}
					}
				}
	
				if(tpObject == predicate) { //line 12
					if(betaObjectExpressions.size() == 1) {
						for(betaObjectExpression <- betaObjectExpressions) {
							val exp = new ZExpression("=", betaObjectExpression, betaPredicateExpression);
							exps = exps ++ Set(exp);
						}
					}
				}
	
				MorphSQLUtility.combineExpresions(exps.toList, Constants.SQL_LOGICAL_OPERATOR_AND);
			}
		  
		}
		
		result;
	}


	def genCondSQLSubject(tp:Triple , alphaResult:MorphAlphaResult, betaGenerator:MorphBaseBetaGenerator 
	    , cm:MorphBaseClassMapping ) : ZExpression = {
		
		val subject = tp.getSubject();
		val betaSubjectSelectItems = betaGenerator.calculateBetaSubject(tp, cm, alphaResult);
		val betaSubjectExpressions = betaSubjectSelectItems.map(
		    betaSubjectSelectItem => betaSubjectSelectItem.getExpression())

		val result1 : ZExpression = {
			if(!subject.isVariable()) {
				if(subject.isURI()) {
					val tpSubject = tp.getSubject();
					val condSQLSubjectURI = this.genCondSQLSubjectURI(tpSubject, alphaResult, cm);
					condSQLSubjectURI
				} else if(subject.isLiteral()) {
					logger.warn("Literal as subject is not supported!");
					val literalValue = subject.getLiteralValue();
					val resultAux2 = {
						literalValue match {
						  case literalValueString:String => {
							new ZExpression("=", betaSubjectExpressions.get(0)
							    , new ZConstant(subject.toString(), ZConstant.STRING));					    
						  }
						  case literalValueDouble:java.lang.Double=> {
							new ZExpression("=", betaSubjectExpressions.get(0)
									, new ZConstant(subject.toString(), ZConstant.NUMBER));					    
						  }
						  case _=> {
							new ZExpression("=", betaSubjectExpressions.get(0)
									, new ZConstant(subject.toString(), ZConstant.STRING));					    
						  }
						}
					}
					resultAux2;
				} else {
				  null
				}
			} else {
				null
			}
		} 

		result1;
	}

	def genCondSQLSubjectURI(tpSubject:Node , alphaResult:MorphAlphaResult, cm:MorphBaseClassMapping) : ZExpression;

	def  genCondSQLSTG(stg:List[Triple], alphaResult:MorphAlphaResult , betaGenerator:MorphBaseBetaGenerator 
			, cm:MorphBaseClassMapping ) : MorphCondSQLResult = {

		//var exps : Set[ZExpression] = Set.empty;
		val firstTriple = stg.get(0);

		val condSubject = this.genCondSQLSubject(firstTriple
				, alphaResult, betaGenerator, cm);

		var condSTGPredicateObject : Set[ZExpression] = Set.empty;
		
		for(i <- 0 until stg.size()) {
			val iTP = stg.get(i);

			val iTPPredicate = iTP.getPredicate();
			if(!iTPPredicate.isURI()) {
				val errorMessage = "Only bounded predicate is not supported in triple : " + iTP;
				logger.warn(errorMessage);
				throw new Exception(errorMessage);
			}
			val iTPPredicateURI = iTPPredicate.getURI();
			
			val mappedClassURIs = cm.getMappedClassURIs();
			val processableTriplePattern = {
				if(iTP.getObject().isURI()) {
					val objectURI = iTP.getObject().getURI();
					if(RDF.`type`.getURI().equals(iTPPredicateURI) && mappedClassURIs.contains(objectURI)) {
						false;
					} else {
					  true;
					}
				} else {
				  true;
				}			  
			}

			
			if(processableTriplePattern) {
				val condPredicateObject = this.genCondSQLPredicateObject(iTP, alphaResult, betaGenerator, cm, iTPPredicateURI);
				//condSQLTB.add(condPredicateObject);
				if(condPredicateObject != null) {
					condSTGPredicateObject = condSTGPredicateObject ++ Set(condPredicateObject);
				}

				for(j <- i+1 until stg.size()) {
					val jTP = stg.get(j);

					val jTPPredicate = jTP.getPredicate();
					if(jTPPredicate.isVariable()) {
						val errorMessage = "Unbounded predicate is not permitted in triple : " + jTP;
						logger.warn(errorMessage);
					}

					if(jTPPredicate.isURI() &&  RDF.`type`.getURI().equals(jTPPredicate.getURI())) {

					} else {
						val expsPredicateObject = this.genCondSQL(
								iTP, jTP, alphaResult, betaGenerator, cm);
						if(expsPredicateObject != null) {
							condSTGPredicateObject = condSTGPredicateObject ++ Set(expsPredicateObject);	
						}
					}
				}					
			}
		}

		val exps = Set(condSubject) ++ condSTGPredicateObject
		val genCondSQLSTG = MorphSQLUtility.combineExpresions(exps, Constants.SQL_LOGICAL_OPERATOR_AND);
		new MorphCondSQLResult(genCondSQLSTG);
	}

	private def genCondSQL(tp1:Triple , tp2:Triple ,alphaResult:MorphAlphaResult 
	    , betaGenerator:MorphBaseBetaGenerator , cm:MorphBaseClassMapping ) : ZExpression = {
		var exps : Set[ZExpression] = Set.empty;

		val tp1Subject = tp1.getSubject();
		val betaSubjectSelectItems = betaGenerator.calculateBetaSubject(
				tp1, cm, alphaResult);
		val betaSub1Exps = betaSubjectSelectItems.map(betaSubjectSelectItem => {
			betaSubjectSelectItem.getExpression()
		})
		
		val tp1Predicate = tp1.getPredicate();
		val betaPre1Exp = betaGenerator.calculateBetaPredicate(tp1Predicate.getURI()).getExpression();

		val tp1Object = tp1.getObject();
		val betaObj1SelectItems = betaGenerator.calculateBetaObject(
				tp1, cm, tp1Predicate.getURI(), alphaResult);
		val betaObj1Exps = betaObj1SelectItems.map(betaObj1SelectItem => {
			if(betaObj1SelectItem.isExpression()) {
				betaObj1SelectItem.getExpression();
			} else {
				new ZConstant(betaObj1SelectItem.toString(), ZConstant.COLUMNNAME);
			}		  
		})
		


		val tp2Predicate = tp2.getPredicate();
		val betaPre2Exp = betaGenerator.calculateBetaPredicate(tp2Predicate.getURI()).getExpression();

		val tp2Object = tp2.getObject();
		val betaObj2SelectItems = betaGenerator.calculateBetaObject(
				tp2, cm, tp2Predicate.getURI(), alphaResult);
		val betaObj2Exps = betaObj2SelectItems.map(betaObj2SelectItem => {
			if(betaObj2SelectItem.isExpression()) {
				betaObj2SelectItem.getExpression();
			} else {
				new ZConstant(betaObj2SelectItem.toString(), ZConstant.COLUMNNAME);
			}		  
		})
		


		if(tp1Subject.toString().equals(tp2Predicate.toString())) {
			if(betaSub1Exps.size() == 1) {
				val exp = new ZExpression("=", betaSub1Exps.get(0), betaPre2Exp);
				exps = exps ++ Set(exp);				
			}
		}

		if(tp1Subject.toString().equals(tp2Object.toString())) {
			if(betaSub1Exps.size() == betaObj2Exps.size()) {
				for(i <- 0 until betaSub1Exps.size()) {
					val exp = new ZExpression("=", betaSub1Exps.get(i), betaObj2Exps.get(i));
					exps = exps ++ Set(exp);					
				}
			}
		}

		if(tp1Predicate.toString().equals(tp2Object.toString())) {
			if(betaObj2Exps.size() == 1) {
				val exp = new ZExpression("=", betaPre1Exp, betaObj2Exps.get(0));
				exps = exps ++ Set(exp);				
			}
		}

		if(tp1Object.toString().equals(tp2Predicate.toString())) {
			if(betaObj1Exps.size() == 1) {
				val exp = new ZExpression("="
						, betaObj1Exps.get(0)
						, betaPre2Exp);
				exps = exps ++ Set(exp);				
			}
		}

		if(tp1Object.toString().equals(tp2Object.toString())) {
			if(betaObj1Exps.size() == betaObj2Exps.size()) {
				for(i <- 0 until betaObj1Exps.size()) {
					val exp = new ZExpression("=", betaObj1Exps.get(i), betaObj2Exps.get(i));
					exps = exps ++ Set(exp);					
				}
			}
		}


		val result = MorphSQLUtility.combineExpresions(exps.toList, Constants.SQL_LOGICAL_OPERATOR_AND);
		result;
	}

	def genCondSQLPredicateObject(tp:Triple ,alphaResult:MorphAlphaResult , betaGenerator:MorphBaseBetaGenerator 
	    ,cm:MorphBaseClassMapping , pm:MorphBasePropertyMapping ) : ZExpression 
	
}