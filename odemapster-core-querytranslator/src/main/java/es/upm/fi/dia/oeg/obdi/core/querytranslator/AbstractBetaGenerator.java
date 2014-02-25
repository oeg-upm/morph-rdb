package es.upm.fi.dia.oeg.obdi.core.querytranslator;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import Zql.ZConstant;
import Zql.ZSelectItem;

import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.vocabulary.RDF;

import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping;
import es.upm.fi.dia.oeg.obdi.core.querytranslator.AbstractQueryTranslator.POS;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLSelectItem;


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
			result = this.calculateBetaSubject(cm, alphaResult);
		} else if(pos == POS.pre) {
			result = new ArrayList<ZSelectItem>();
			result.add(this.calculateBetaPredicate(predicateURI));
		} else if(pos == POS.obj) {
			boolean predicateIsRDFSType = RDF.type.getURI().equals(predicateURI);
			if(predicateIsRDFSType) {
				ZConstant className = new ZConstant(
						cm.getConceptName(), ZConstant.STRING);
				SQLSelectItem selectItem = new SQLSelectItem();
				selectItem.setExpression(className);
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

	public abstract List<ZSelectItem> calculateBetaObject(Triple triple
			, AbstractConceptMapping cm, String predicateURI, AlphaResult alphaResult)
	throws QueryTranslationException;

	public ZSelectItem calculateBetaPredicate(String predicateURI) {
		ZConstant predicateURIConstant = 
				new ZConstant(predicateURI, ZConstant.STRING);
		SQLSelectItem selectItem = new SQLSelectItem();
		selectItem.setExpression(predicateURIConstant);
		return selectItem;
	}
	
	public abstract List<ZSelectItem> calculateBetaSubject(AbstractConceptMapping cm, AlphaResult alphaResult);

	protected AbstractQueryTranslator getOwner() {
		return owner;
	}

	
}
