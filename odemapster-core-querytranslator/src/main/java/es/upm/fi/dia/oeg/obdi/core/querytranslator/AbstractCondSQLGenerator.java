package es.upm.fi.dia.oeg.obdi.core.querytranslator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import Zql.ZConstant;
import Zql.ZExp;
import Zql.ZExpression;
import Zql.ZSelectItem;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.vocabulary.RDF;

import es.upm.fi.dia.oeg.morph.base.ColumnMetaData;
import es.upm.fi.dia.oeg.morph.base.Constants;
import es.upm.fi.dia.oeg.morph.base.MorphSQLUtility;
import es.upm.fi.dia.oeg.morph.base.MorphTriple;
import es.upm.fi.dia.oeg.morph.base.SPARQLUtility;
import es.upm.fi.dia.oeg.obdi.core.exception.InsatisfiableSQLExpression;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractPropertyMapping;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLUtility;
import es.upm.fi.dia.oeg.upm.morph.sql.MorphSQLConstant;

public abstract class AbstractCondSQLGenerator {
	private static Logger logger = Logger.getLogger(AbstractCondSQLGenerator.class);
	//protected AbstractBetaGenerator betaGenerator;
	protected boolean ignoreRDFTypeStatement = false;
	protected AbstractQueryTranslator owner;
	
	public AbstractCondSQLGenerator(AbstractQueryTranslator owner) {
		super();
		this.owner = owner;
	}

	public CondSQLResult genCondSQL(Triple tp
			, AlphaResult alphaResult, AbstractBetaGenerator betaGenerator
			, AbstractConceptMapping cm, String predicateURI) throws QueryTranslationException, InsatisfiableSQLExpression {

		ZExpression condSQLSubject = this.genCondSQLSubject(tp, alphaResult, betaGenerator, cm);

		ZExpression condSQLPredicateObject = null;
		try {
			condSQLPredicateObject = this.genCondSQLPredicateObject(tp, alphaResult, betaGenerator, cm, predicateURI);			
		} catch(InsatisfiableSQLExpression e) {
			throw e;
		} catch(Exception e) {
			logger.error(e.getMessage());
		}

		ZExpression condSQL = null;
		if(condSQLSubject == null && condSQLPredicateObject==null) {
			condSQL = null;
		} else if(condSQLSubject != null && condSQLPredicateObject==null) {
			condSQL = condSQLSubject;
		} else if(condSQLSubject == null && condSQLPredicateObject!=null) {
			condSQL = condSQLPredicateObject;
		} else {
			condSQL = new ZExpression("AND", condSQLSubject, condSQLPredicateObject);
		}

		logger.debug("genCondSQL = " + condSQL);
		return new CondSQLResult(condSQL);
	}



	public ZExpression generateIsNotNullExpression(ZExp betaObjectExpression) {
		
		ZExpression exp = new ZExpression("IS NOT NULL");
		exp.addOperand(betaObjectExpression);
		return exp;
	}

	protected ZExpression genCondSQLPredicateObject(Triple tp
			,AlphaResult alphaResult, AbstractBetaGenerator betaGenerator
			, AbstractConceptMapping cm, String predicateURI) throws QueryTranslationException, InsatisfiableSQLExpression {
		ZExpression result = null;
		Collection<ZExpression> exps = new HashSet<ZExpression>();
		Map<String, ColumnMetaData> mapColumnMetaData = cm.getLogicalTable().getColumnsMetaData();
		boolean isRDFTypeStatement = RDF.type.getURI().equals(predicateURI);

		Collection<AbstractPropertyMapping> pms = 
				cm.getPropertyMappings(predicateURI);
		int pmsSize = pms.size();
		if(pms == null || pmsSize == 0) {
			if(!isRDFTypeStatement) {
				String errorMessage = "No mappings found for predicate : " + predicateURI;
				throw new QueryTranslationException(errorMessage);				
			} 
		} else if(pms.size() > 1) {
			String errorMessage = "Multiple mappings are not permitted for predicate " + predicateURI;
			throw new QueryTranslationException(errorMessage);
		} if(pms.size() == 1) {
			Node subject = tp.getSubject();
			Node predicate = tp.getPredicate();
			Node object = tp.getObject();
			
			AbstractPropertyMapping pm = pms.iterator().next();
			ZExpression result1 = this.genCondSQLPredicateObject(tp, alphaResult, betaGenerator, cm, pm);
			if(result1 != null) {
				exps.add(result1);	
			}
			

			List<ZSelectItem> betaSubjectSelectItems = 
					betaGenerator.calculateBetaSubject(tp, cm, alphaResult);
			List<ZExp> betaSubjectExpressions = new ArrayList<ZExp>();
			for(ZSelectItem betaSubjectSelectItem : betaSubjectSelectItems) {
				ZExp betaSubjectExpression = betaSubjectSelectItem.getExpression();
				betaSubjectExpressions.add(betaSubjectExpression);
			}
			
			ZExp betaPredicateExpression = 
					betaGenerator.calculateBetaPredicate(predicateURI).getExpression();
			
			List<ZSelectItem> betaObjectSelectItems = 
					betaGenerator.calculateBetaObject(tp, cm, predicateURI, alphaResult);
			List<ZExp> betaObjectExpressions = new ArrayList<ZExp>();
			for(ZSelectItem betaObjectSelectItem : betaObjectSelectItems) {
				ZExp betaObjectExp;
				if(betaObjectSelectItem.isExpression()) {
					betaObjectExp = betaObjectSelectItem.getExpression();
				} else {
					betaObjectExp = new ZConstant(betaObjectSelectItem.toString()
							, ZConstant.COLUMNNAME);
				}
				betaObjectExpressions.add(betaObjectExp);
			}
			
			

			if(!predicate.isVariable()) { //line 08
					new ZExpression("="
							, betaPredicateExpression
							, new ZConstant(predicate.toString(), ZConstant.STRING));				
			}

			if(!object.isVariable()) { //line 09
				ZExp exp = null;

				if(object.isURI()) {
					ZConstant objConstant = new ZConstant(object.getURI(), ZConstant.STRING);
					for(ZExp betaObjectExpression : betaObjectExpressions) {
						exp = new ZExpression("=", betaObjectExpression, objConstant);	
					}
				} else if(object.isLiteral()) {
					Object literalValue = object.getLiteralValue();
					if(literalValue instanceof String) {
						ZConstant objConstant = new ZConstant(literalValue.toString(), ZConstant.STRING);
						for(ZExp betaObjectExpression : betaObjectExpressions) {
							exp = new ZExpression("=", betaObjectExpression, objConstant);	
						}
					} else if (literalValue instanceof Double) {
						ZConstant objConstant = new ZConstant(literalValue.toString(), ZConstant.NUMBER);
						for(ZExp betaObjectExpression : betaObjectExpressions) {
							exp = new ZExpression("=", betaObjectExpression, objConstant);	
						}
					} else {
						for(ZExp betaObjectExpression : betaObjectExpressions) {
							ZConstant objConstant = new ZConstant(literalValue.toString(), ZConstant.STRING);
							exp = new ZExpression("="
									, betaObjectExpression
									, objConstant);						
						}
					}
				}

				if(exp != null) {
					//result = new ZExpression("AND", result, exp);
				}

			} else { //object.isVariable() // improvement by Freddy
				if(!SPARQLUtility.isBlankNode(object)) {
					boolean isSingleTripleFromTripleBlock = false;

					if(tp instanceof MorphTriple) {
						MorphTriple etp = (MorphTriple) tp;
						if(etp.isSingleTripleFromTripleBlock()) {
							isSingleTripleFromTripleBlock = true;
						} 
					} 

					//for dealing with unbound() function, we should remove this part
					if(!isSingleTripleFromTripleBlock) {
						
						for(ZExp betaObjectExpression : betaObjectExpressions) {
							if(betaObjectExpression instanceof ZConstant) {
								ZConstant betaObjectZConstant = (ZConstant) betaObjectExpression;
//								String betaValue = betaObjectZConstant.getValue();
//								SQLSelectItem betaColumnSelectItem = new SQLSelectItem(betaValue);
//								String betaColumn = betaColumnSelectItem.getColumn();
								
								MorphSQLConstant betaColumnConstant = MorphSQLConstant.apply(betaObjectZConstant);
								String betaColumn = betaColumnConstant.column();

								ColumnMetaData cmd = null;
								if(mapColumnMetaData != null) {
									cmd = mapColumnMetaData.get(betaColumn);	
								}
								
								if(cmd == null || cmd.isNullable()) {
									ZExpression exp = this.generateIsNotNullExpression(betaObjectExpression);
									if(exp != null) {
										exps.add(exp);
									}							
								}
							}
						}
					}					
				}
			}
			
			if(subject == predicate) { //line 10
				if(betaSubjectExpressions.size() == 1) {
					for(int i=0; i<betaSubjectExpressions.size(); i++) {
						ZExp betaSubjectExpression = betaSubjectExpressions.get(i);
						
						ZExpression exp = new ZExpression("="
								, betaSubjectExpression
								, betaPredicateExpression);
						exps.add(exp);					
					}
				}
			}

			if(subject == object) { //line 11
				if(betaSubjectExpressions.size() == betaObjectExpressions.size()) {
					for(int i=0; i<betaSubjectExpressions.size(); i++) {
						ZExp betaSubjectExpression = betaSubjectExpressions.get(i);
						ZExp betaObjectExpression = betaObjectExpressions.get(i);
						ZExpression exp = new ZExpression("="
								, betaSubjectExpression
								, betaObjectExpression);
						exps.add(exp);					
					}
				}
			}

			if(object == predicate) { //line 12
				if(betaObjectExpressions.size() == 1) {
					for(int i=0; i<betaObjectExpressions.size(); i++) {
						ZExp betaObjectExpression = betaObjectExpressions.get(i);
						ZExpression exp = new ZExpression("="
								, betaObjectExpression
								, betaPredicateExpression);
						exps.add(exp);					
					}
				}
			}

			result = MorphSQLUtility.combineExpresions(
					exps, Constants.SQL_LOGICAL_OPERATOR_AND());
			
		}
		return result;
	}


	protected ZExpression genCondSQLSubject(Triple tp, AlphaResult alphaResult 
			, AbstractBetaGenerator betaGenerator, AbstractConceptMapping cm) throws QueryTranslationException {
		ZExpression result1 = null;
		Node subject = tp.getSubject();
		//ZSelectItem betaCMSelectItem = betaGenerator.calculateBeta(tp, POS.sub);
		List<ZSelectItem> betaSubjectSelectItems = betaGenerator.calculateBetaSubject(
				tp, cm, alphaResult);
		List<ZExp> betaSubjectExpressions = new ArrayList<ZExp>();
		for(ZSelectItem betaSubjectSelectItem : betaSubjectSelectItems) {
			ZExp betaSubjectExpression = betaSubjectSelectItem.getExpression();
			betaSubjectExpressions.add(betaSubjectExpression);
		}

		if(!subject.isVariable()) {
			if(subject.isURI()) {
				Node tpSubject = tp.getSubject();
				result1 = this.genCondSQLSubjectURI(tpSubject, alphaResult, cm);
			} else if(subject.isLiteral()) {
				logger.warn("Literal as subject is not supported!");
				Object literalValue = subject.getLiteralValue();
				if(literalValue instanceof String) {
					result1 = new ZExpression("="
							, betaSubjectExpressions.get(0)
							, new ZConstant(subject.toString(), ZConstant.STRING));				
				} else if (literalValue instanceof Double) {
					result1 = new ZExpression("="
							, betaSubjectExpressions.get(0)
							, new ZConstant(subject.toString(), ZConstant.NUMBER));
				} else {
					result1 = new ZExpression("="
							, betaSubjectExpressions.get(0)
							, new ZConstant(subject.toString(), ZConstant.STRING));				
				}
			}
		}

		return result1;

	}

	protected abstract ZExpression genCondSQLSubjectURI(Node tpSubject,
			AlphaResult alphaResult, AbstractConceptMapping cm) 
					throws QueryTranslationException;

	public ZExpression genCondSQLSTG(List<Triple> stg
			, AlphaResult alphaResult, AbstractBetaGenerator betaGenerator
			, AbstractConceptMapping cm) 
					throws Exception {

		Collection<ZExpression> exps = new HashSet<ZExpression>();
		Triple firstTriple = stg.get(0);

		ZExpression condSubject = this.genCondSQLSubject(firstTriple
				, alphaResult, betaGenerator, cm);
		if(condSubject != null) {
			exps.add(condSubject);
			//condSQLTB.add(condSubject);
		} 

		for(int i=0; i<stg.size(); i++) {
			Triple iTP = stg.get(i);

			Node iTPPredicate = iTP.getPredicate();
			if(!iTPPredicate.isURI()) {
				String errorMessage = "Only bounded predicate is not supported in triple : " + iTP;
				logger.warn(errorMessage);
				throw new QueryTranslationException(errorMessage);
			}
			String iTPPredicateURI = iTPPredicate.getURI();
			
			Collection<String> mappedClassURIs = cm.getMappedClassURIs();
			boolean processableTriplePattern = true;
			if(iTP.getObject().isURI()) {
				String objectURI = iTP.getObject().getURI();
				if(RDF.type.getURI().equals(iTPPredicateURI) && mappedClassURIs.contains(objectURI)) {
					processableTriplePattern = false;
				}
			}
			
			if(processableTriplePattern) {
				ZExpression condPredicateObject = this.genCondSQLPredicateObject(
						iTP, alphaResult, betaGenerator, cm, iTPPredicateURI);
				//condSQLTB.add(condPredicateObject);
				if(condPredicateObject != null) {
					exps.add(condPredicateObject);
				}

				for(int j=i+1; j<stg.size(); j++) {
					Triple jTP = stg.get(j);

					Node jTPPredicate = jTP.getPredicate();
					if(jTPPredicate.isVariable()) {
						String errorMessage = "Unbounded predicate is not permitted in triple : " + jTP;
						logger.warn(errorMessage);
					}

					if(jTPPredicate.isURI() &&  this.ignoreRDFTypeStatement 
							&& RDF.type.getURI().equals(jTPPredicate.getURI())) {

					} else {
						ZExpression expsPredicateObject = this.genCondSQL(
								iTP, jTP, alphaResult, betaGenerator, cm);
						if(expsPredicateObject != null) {
							exps.add(expsPredicateObject);	
						}
					}
				}					
			}
		}

		ZExpression result = MorphSQLUtility.combineExpresions(
				exps, Constants.SQL_LOGICAL_OPERATOR_AND());
		logger.debug("genCondSQLTB = " + result);
		return result;

	}

	public boolean isIgnoreRDFTypeStatement() {
		return ignoreRDFTypeStatement;
	}

	public void setIgnoreRDFTypeStatement(boolean ignoreRDFTypeStatement) {
		this.ignoreRDFTypeStatement = ignoreRDFTypeStatement;
	}

	private ZExpression genCondSQL(Triple tp1, Triple tp2, 
			AlphaResult alphaResult, AbstractBetaGenerator betaGenerator, AbstractConceptMapping cm
			) throws Exception {
		Collection<ZExpression> exps = new HashSet<ZExpression>();

		Node tp1Subject = tp1.getSubject();
		List<ZSelectItem> betaSubjectSelectItems = betaGenerator.calculateBetaSubject(
				tp1, cm, alphaResult);
		List<ZExp> betaSub1Exps = new ArrayList<ZExp>();
		for(ZSelectItem betaSubjectSelectItem : betaSubjectSelectItems) {
			betaSub1Exps.add(betaSubjectSelectItem.getExpression());
		}
		
		Node tp1Predicate = tp1.getPredicate();
		ZExp betaPre1Exp = betaGenerator.calculateBetaPredicate(
				tp1Predicate.getURI()).getExpression();

		Node tp1Object = tp1.getObject();
		List<ZSelectItem> betaObj1SelectItems = betaGenerator.calculateBetaObject(
				tp1, cm, tp1Predicate.getURI(), alphaResult);
		List<ZExp> betaObj1Exps = new ArrayList<ZExp>();
		for(ZSelectItem betaObj1SelectItem : betaObj1SelectItems) {
			ZExp betaObj1Exp;
			if(betaObj1SelectItem.isExpression()) {
				betaObj1Exp = betaObj1SelectItem.getExpression();
			} else {
				betaObj1Exp = new ZConstant(betaObj1SelectItem.toString(), ZConstant.COLUMNNAME);
			}
			betaObj1Exps.add(betaObj1Exp);
		}

		Node tp2Predicate = tp2.getPredicate();
		ZExp betaPre2Exp = betaGenerator.calculateBetaPredicate(tp2Predicate.getURI()).getExpression();

		
		Node tp2Object = tp2.getObject();
		List<ZSelectItem> betaObj2SelectItems = betaGenerator.calculateBetaObject(
				tp2, cm, tp2Predicate.getURI(), alphaResult);
		List<ZExp> betaObj2Exps = new ArrayList<ZExp>();
		for(ZSelectItem betaObj2SelectItem : betaObj2SelectItems) {
			ZExp betaObj2Exp;
			if(betaObj2SelectItem.isExpression()) {
				betaObj2Exp = betaObj2SelectItem.getExpression();
			} else {
				betaObj2Exp = new ZConstant(betaObj2SelectItem.toString(), ZConstant.COLUMNNAME);
			}
			betaObj2Exps.add(betaObj2Exp);
		}

		if(tp1Subject.toString().equals(tp2Predicate.toString())) {
			if(betaSub1Exps.size() == 1) {
				ZExpression exp = new ZExpression("="
						, betaSub1Exps.get(0)
						, betaPre2Exp);
				exps.add(exp);				
			}
		}

		if(tp1Subject.toString().equals(tp2Object.toString())) {
			if(betaSub1Exps.size() == betaObj2Exps.size()) {
				for(int i=0;i<betaSub1Exps.size();i++) {
					ZExpression exp = new ZExpression("="
							, betaSub1Exps.get(i)
							, betaObj2Exps.get(i));
					exps.add(exp);					
				}
			}
		}

		if(tp1Predicate.toString().equals(tp2Object.toString())) {
			if(betaObj2Exps.size() == 1) {
				ZExpression exp = new ZExpression("="
						, betaPre1Exp
						, betaObj2Exps.get(0));
				exps.add(exp);				
			}
		}

		if(tp1Object.toString().equals(tp2Predicate.toString())) {
			if(betaObj1Exps.size() == 1) {
				ZExpression exp = new ZExpression("="
						, betaObj1Exps.get(0)
						, betaPre2Exp);
				exps.add(exp);				
			}
		}

		if(tp1Object.toString().equals(tp2Object.toString())) {
			if(betaObj1Exps.size() == betaObj2Exps.size()) {
				for(int i=0; i<betaObj1Exps.size();i++) {
					ZExpression exp = new ZExpression("="
							, betaObj1Exps.get(i)
							, betaObj2Exps.get(i));
					exps.add(exp);					
				}
			}
		}


		ZExpression result = MorphSQLUtility.combineExpresions(
				exps, Constants.SQL_LOGICAL_OPERATOR_AND());
		return result;
	}

	protected abstract ZExpression genCondSQLPredicateObject(Triple tp,
			AlphaResult alphaResult, AbstractBetaGenerator betaGenerator,
			AbstractConceptMapping cm, AbstractPropertyMapping pm)
					throws QueryTranslationException, InsatisfiableSQLExpression;

}
