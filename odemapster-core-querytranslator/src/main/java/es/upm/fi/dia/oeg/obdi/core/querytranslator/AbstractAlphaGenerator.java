package es.upm.fi.dia.oeg.obdi.core.querytranslator;

import java.util.Collection;
import java.util.List;
import java.util.Vector;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.vocabulary.RDF;

import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractPropertyMapping;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLJoinTable;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLLogicalTable;

public abstract class AbstractAlphaGenerator {
//	private static Logger logger = Logger.getLogger(AbstractAlphaGenerator.class);
	
	protected AbstractQueryTranslator owner; 
	protected boolean ignoreRDFTypeStatement = false;
	protected boolean subqueryAsView = false;
	
	public AbstractAlphaGenerator(AbstractQueryTranslator owner) {
		super();
		this.owner = owner;
	}

//	public AlphaResultUnion calculateAlpha(Triple tp, AbstractConceptMapping cm) throws Exception {
//		AlphaResultUnion result = null;
//		
//		Node tpPredicate = tp.getPredicate();
//		if(tpPredicate.isURI()) {
//			String predicateURI = tpPredicate.getURI();
//			AlphaResult alphaResult = this.calculateAlpha(tp, cm, predicateURI);
////			String logicalTableAlias = cm.getLogicalTableAlias();
////			if(logicalTableAlias == null || logicalTableAlias.equals("")) {
////				cm.setLogicalTableAlias(alphaResult.getAlphaSubject().getAlias());
////			}
//			result = new AlphaResultUnion(alphaResult);
//		} else if(tpPredicate.isVariable()) {
//			Collection<AbstractPropertyMapping> pms = cm.getPropertyMappings();
//			if(pms != null && pms.size() > 0) {
//				List<AlphaResult> alphaResults = new Vector<AlphaResult>();
//				for(AbstractPropertyMapping pm : pms) {
//					String predicateURI = pm.getMappedPredicateName();
//					AlphaResult alphaResult = this.calculateAlpha(tp, cm, predicateURI);
//					alphaResults.add(alphaResult);
//				}
//				result = new AlphaResultUnion(alphaResults);
//			} else {
//				//TODO : deal when no predicateobjectmap is specified, but only subjectmap
////				SQLLogicalTable alphaSubject = (SQLLogicalTable) this.calculateAlphaSubject(tp.getSubject(), cm);
////				AlphaResult alphaResult = new AlphaResult(alphaSubject, null, null);
////				result = new AlphaResultUnion(alphaResult);	
//			}
//		} else {
//			String errorMessage = "Predicate has to be either an URI or a variable";
//			throw new QueryTranslationException(errorMessage);
//		}
//		
//		
//		logger.debug("alpha(tp) = " + result);
//		return result;
//	}
	
	public abstract AlphaResult calculateAlpha(Triple tp
			, AbstractConceptMapping abstractConceptMapping, String predicateURI) 
					throws QueryTranslationException;

	public abstract AlphaResult calculateAlpha(Triple tp
			, AbstractConceptMapping abstractConceptMapping, String predicateURI, AbstractPropertyMapping pm) 
					throws QueryTranslationException;

	protected abstract SQLJoinTable calculateAlphaPredicateObject(Triple triple
			, AbstractConceptMapping abstractConceptMapping, AbstractPropertyMapping pm 
			, String logicalTableAlias) throws Exception;
	
	protected abstract SQLLogicalTable calculateAlphaSubject(
			Node subject, AbstractConceptMapping abstractConceptMapping) throws Exception;
	
	public List<AlphaResultUnion> calculateAlphaSTG(
			Collection<Triple> triples, AbstractConceptMapping cm)
			throws Exception {
		List<AlphaResultUnion> alphaResultUnionList = new Vector<AlphaResultUnion>();
		
		Triple firstTriple = triples.iterator().next();
		Node tpSubject = firstTriple.getSubject();
		SQLLogicalTable alphaSubject = this.calculateAlphaSubject(tpSubject, cm);
		//String logicalTableAlias = cm.getLogicalTableAlias();
		String logicalTableAlias = alphaSubject.getAlias();
//		if(logicalTableAlias == null || logicalTableAlias.equals("")) {
//			cm.setLogicalTableAlias(alphaSubject.getAlias());
//		}
		
		//mapping projection of corresponding predicates
		
		for(Triple tp : triples) {
			Node tpPredicate = tp.getPredicate();
			List<SQLJoinTable> alphaPredicateObjects = new Vector<SQLJoinTable>();
			List<SQLLogicalTable> alphaPredicateObjects2 = new Vector<SQLLogicalTable>();
			if(tpPredicate.isURI()) {
				String tpPredicateURI = tpPredicate.getURI();

				Collection<String> mappedClassURIs = cm.getMappedClassURIs();
				boolean processableTriplePattern = true;
				if(tp.getObject().isURI()) {
					String objectURI = tp.getObject().getURI();
					if(RDF.type.getURI().equals(tpPredicateURI) && mappedClassURIs.contains(objectURI)) {
						processableTriplePattern = false;
					}
				}
				
				if(processableTriplePattern) {
					List<SQLJoinTable> alphaPredicateObjectAux = calculateAlphaPredicateObjectSTG(
							tp, cm, tpPredicateURI,logicalTableAlias);
					if(alphaPredicateObjectAux != null) {
						alphaPredicateObjects.addAll(alphaPredicateObjectAux);	
					}

					List<SQLLogicalTable> alphaPredicateObjectAux2 = calculateAlphaPredicateObjectSTG2(
							tp, cm, tpPredicateURI,logicalTableAlias);
					if(alphaPredicateObjectAux2 != null) {
						alphaPredicateObjects2.addAll(alphaPredicateObjectAux2);	
					}

					AlphaResult alphaResult = new AlphaResult(alphaSubject
							, alphaPredicateObjects, tpPredicateURI);
					//alphaResult.setAlphaPredicateObjects2(alphaPredicateObjectAux2);
					
					AlphaResultUnion alphaTP = new AlphaResultUnion(alphaResult);
					alphaResultUnionList.add(alphaTP);					
				}
			} else if(tpPredicate.isVariable()){
				Collection<AbstractPropertyMapping> pms = cm.getPropertyMappings();
				AlphaResultUnion alphaTP = new AlphaResultUnion();
				for(AbstractPropertyMapping pm : pms) {
					String tpPredicateURI = pm.getMappedPredicateName();
					List<SQLJoinTable> alphaPredicateObjectAux = calculateAlphaPredicateObjectSTG(
							tp, cm, tpPredicateURI,logicalTableAlias);
					if(alphaPredicateObjectAux != null) {
						alphaPredicateObjects.addAll(alphaPredicateObjectAux);	
					}
					
					List<SQLLogicalTable> alphaPredicateObjectAux2 = calculateAlphaPredicateObjectSTG2(
							tp, cm, tpPredicateURI,logicalTableAlias);					
					if(alphaPredicateObjectAux2 != null) {
						alphaPredicateObjects2.addAll(alphaPredicateObjectAux2);	
					}

					AlphaResult alphaResult = new AlphaResult(alphaSubject
							, alphaPredicateObjects, tpPredicateURI);
					//alphaResult.setAlphaPredicateObjects2(alphaPredicateObjectAux2);
					
					alphaTP.add(alphaResult);
				}
				
				if(alphaTP != null) {
					alphaResultUnionList.add(alphaTP);	
				}
				
			} else {
				String errorMessage = "Predicate has to be either an URI or a variable";
				throw new QueryTranslationException(errorMessage);				
			}

		}

		return alphaResultUnionList;
	}
	
//	public abstract AbstractConceptMapping calculateAlphaCM(Triple tp) throws Exception;
//	public abstract AbstractConceptMapping calculateAlphaCMTB(Collection<Triple> triples) throws Exception;

	public abstract List<SQLJoinTable> calculateAlphaPredicateObjectSTG(Triple tp,
			AbstractConceptMapping cm, String tpPredicateURI,
			String logicalTableAlias) throws Exception;

	public abstract List<SQLLogicalTable> calculateAlphaPredicateObjectSTG2(Triple tp,
			AbstractConceptMapping cm, String tpPredicateURI,
			String logicalTableAlias) throws Exception;

	public boolean isIgnoreRDFTypeStatement() {
		return ignoreRDFTypeStatement;
	}

	public void setIgnoreRDFTypeStatement(boolean ignoreRDFTypeStatement) {
		this.ignoreRDFTypeStatement = ignoreRDFTypeStatement;
	}

	public boolean isSubqueryAsView() {
		return subqueryAsView;
	}

	public void setSubqueryAsView(boolean subqueryAsView) {
		this.subqueryAsView = subqueryAsView;
	}


	protected abstract SQLLogicalTable calculateAlphaPredicateObject2(Triple triple,
			AbstractConceptMapping abstractConceptMapping,
			AbstractPropertyMapping abstractPropertyMapping,
			String logicalTableAlias) throws QueryTranslationException;


	public void setOwner(AbstractQueryTranslator owner) {
		this.owner = owner;
	}



}
