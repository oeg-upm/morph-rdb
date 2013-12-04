package es.upm.fi.dia.oeg.obdi.core.querytranslator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.log4j.Logger;

import Zql.ZConstant;
import Zql.ZSelectItem;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.vocabulary.RDF;

import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractPropertyMapping;
import es.upm.fi.dia.oeg.obdi.core.querytranslator.AbstractQueryTranslator.POS;
import es.upm.fi.dia.oeg.upm.morph.sql.MorphSQLSelectItem;


public abstract class AbstractBetaGenerator {
	private static Logger logger = Logger.getLogger(AbstractBetaGenerator.class);
	protected AbstractQueryTranslator owner;
	protected AbstractAlphaGenerator alphaGenerator;
	
	public AbstractBetaGenerator(AbstractQueryTranslator owner) {
		super();
		this.owner = owner;
	}

	public List<ZSelectItem> calculateBeta(Triple tp, POS pos
			, AbstractConceptMapping cm, String predicateURI
			, AlphaResult alphaResult) throws QueryTranslationException {
		List<ZSelectItem> result;
		
		if(pos == POS.sub) {
			result = this.calculateBetaSubject(tp, cm, alphaResult);
		} else if(pos == POS.pre) {
			result = new ArrayList<ZSelectItem>();
			result.add(this.calculateBetaPredicate(predicateURI));
		} else if(pos == POS.obj) {
			boolean predicateIsRDFSType = RDF.type.getURI().equals(predicateURI);
			if(predicateIsRDFSType) {
				ZConstant className = new ZConstant(
						cm.getConceptName(), ZConstant.STRING);
				ZSelectItem selectItem = MorphSQLSelectItem.apply(className);
				//selectItem.setExpression(className);
				result = new ArrayList<ZSelectItem>();
				result.add(selectItem);
			} else {
				result = this.calculateBetaObject(
						tp, cm, predicateURI, alphaResult);	
			}
		} else {
			throw new QueryTranslationException("invalid Pos value in beta!");
		}
		
		logger.debug("beta = " + result);
		return result;
	}

	public List<ZSelectItem> calculateBetaObject(Triple triple
			, AbstractConceptMapping cm, String predicateURI, AlphaResult alphaResult)
	throws QueryTranslationException {
		List<ZSelectItem> betaObjects = new ArrayList<ZSelectItem>();
//		String dbType = this.owner.getDatabaseType();
//		Node object = tp.getObject();
		
		//String logicalTableAlias = triplesMap.getLogicalTable().getAlias();
//		String logicalTableAlias = alphaResult.getAlphaSubject().getAlias();
		
		Collection<AbstractPropertyMapping> pms = cm.getPropertyMappings(predicateURI);
		if(pms == null || pms.isEmpty()) {
			String errorMessage = "Undefined mappings for : " + predicateURI 
					+ " for class " + cm.getConceptName();
			logger.debug(errorMessage);
		} else if (pms.size() > 1) {
			String errorMessage = "Multiple property mappings defined, result may be wrong!";
			throw new QueryTranslationException(errorMessage);			
		} else {//if(pms.size() == 1)
			AbstractPropertyMapping pm = pms.iterator().next();
			betaObjects = this.calculateBetaObject(triple, cm, predicateURI, alphaResult, pm);
		}

		return betaObjects;		
	}

	public abstract List<ZSelectItem> calculateBetaObject(Triple triple
			, AbstractConceptMapping cm, String predicateURI, AlphaResult alphaResult, AbstractPropertyMapping pm)
	throws QueryTranslationException;

	public ZSelectItem calculateBetaPredicate(String predicateURI) {
		ZConstant predicateURIConstant = 
				new ZConstant(predicateURI, ZConstant.STRING);
//		ZSelectItem selectItem = new SQLSelectItem();
//		selectItem.setExpression(predicateURIConstant);
		ZSelectItem selectItem = MorphSQLSelectItem.apply(predicateURIConstant);
		return selectItem;
	}
	
	public abstract List<ZSelectItem> calculateBetaSubject(Triple tp, AbstractConceptMapping cm, AlphaResult alphaResult);

	protected AbstractQueryTranslator getOwner() {
		return owner;
	}

	
}
