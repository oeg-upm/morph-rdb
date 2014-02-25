package es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.querytranslator;

import java.util.Collection;
import java.util.Vector;

import org.apache.log4j.Logger;

import Zql.ZConstant;
import Zql.ZSelectItem;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

import es.upm.fi.dia.oeg.morph.base.Constants;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractPropertyMapping;
import es.upm.fi.dia.oeg.obdi.core.querytranslator.AbstractBetaGenerator;
import es.upm.fi.dia.oeg.obdi.core.querytranslator.AbstractPRSQLGenerator;
import es.upm.fi.dia.oeg.obdi.core.querytranslator.AbstractQueryTranslator;
import es.upm.fi.dia.oeg.obdi.core.querytranslator.AlphaResult;
import es.upm.fi.dia.oeg.obdi.core.querytranslator.NameGenerator;
import es.upm.fi.dia.oeg.obdi.core.querytranslator.QueryTranslationException;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLSelectItem;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLObjectMap;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLPredicateObjectMap;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLRefObjectMap;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLSubjectMap;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTriplesMap;

public class R2RMLPRSQLGenerator extends AbstractPRSQLGenerator {
	private static Logger logger = Logger.getLogger(R2RMLPRSQLGenerator.class);
	//private R2RMLQueryTranslator owner;
	private Constants constants = new Constants();
	
	public R2RMLPRSQLGenerator(AbstractQueryTranslator owner) {
		super(owner);
	}
	
//	@Override
//	public Collection<ZSelectItem> genPRSQL(Triple tp, BetaResult betaResult
//			, NameGenerator nameGenerator) throws Exception {
//		Node tpSubject = tp.getSubject();
//		AbstractConceptMapping cmSubject = this.mapInferredTypes.get(tpSubject).iterator().next();
//		return this.genPRSQL(tp, betaResult, nameGenerator, cmSubject);
//	}
	
	@Override
	protected Collection<ZSelectItem> genPRSQLSubject(Triple tp
			, AlphaResult alphaResult, AbstractBetaGenerator betaGenerator
			, NameGenerator nameGenerator, AbstractConceptMapping cmSubject
			) throws QueryTranslationException {
		Collection<ZSelectItem> result = new Vector<ZSelectItem>();
		Collection<ZSelectItem> parentResult = super.genPRSQLSubject(tp, alphaResult
				, betaGenerator, nameGenerator, cmSubject);
		result.addAll(parentResult);
		
		Node subject = tp.getSubject();
		
		if(subject.isVariable()) {
			R2RMLTriplesMap triplesMap = (R2RMLTriplesMap) cmSubject;
			R2RMLSubjectMap subjectMap = triplesMap.getSubjectMap();
//			this.getOwner().getMapVarMapping2().put(subject.getName(), subjectMap);
			Collection<ZSelectItem> childResult = new Vector<ZSelectItem>();
			ZConstant mappingHashCodeConstant = new ZConstant(
					subjectMap.hashCode() + "", ZConstant.NUMBER);
			SQLSelectItem mappingSelectItem = new SQLSelectItem();
			mappingSelectItem.setExpression(mappingHashCodeConstant);
			String mappingSelectItemAlias = constants.PREFIX_MAPPING_ID() + subject.getName();
			mappingSelectItem.setAlias(mappingSelectItemAlias);
			mappingSelectItem.setDbType(this.getOwner().getDatabaseType());
			mappingSelectItem.setColumnType(constants.COLUMN_TYPE_INTEGER());
			childResult.add(mappingSelectItem);
			result.addAll(childResult);
			this.getOwner().getMapHashCodeMapping().put(subjectMap.hashCode(), subjectMap);
		}
		
		return result;
		//ZSelectItem selectItemSubject = betaGenerator.calculateBetaSubject(cmSubject);
	}
	
//	@Override
//	public Collection<ZSelectItem> genPRSQLSTG(List<Triple> tripleBlock,
//			List<BetaResultSet> betaResultSet, NameGenerator nameGenerator, AbstractConceptMapping cm) throws Exception {
//
//		Collection<ZSelectItem> prList = this.genPRSQLSTG(tripleBlock, betaResultSet, nameGenerator, cm);
//		return prList;
//	}

	@Override
	public AbstractQueryTranslator getOwner() {
		return super.owner;
	}

	@Override
	protected Collection<ZSelectItem> genPRSQLObject(Triple tp,
			AlphaResult alphaResult, AbstractBetaGenerator betaGenerator,
			NameGenerator nameGenerator, AbstractConceptMapping cmSubject,
			String predicateURI, String columnType
			)
			throws QueryTranslationException {
		Collection<ZSelectItem> result = new Vector<ZSelectItem>();
		Collection<ZSelectItem> parentResult = super.genPRSQLObject(tp, alphaResult, betaGenerator
				, nameGenerator, cmSubject, predicateURI, columnType);
		result.addAll(parentResult);
		
		Node object = tp.getObject();
		if(object.isVariable()) {
			Collection<ZSelectItem> childResult = new Vector<ZSelectItem>();
			Collection<AbstractPropertyMapping> propertyMappings = cmSubject.getPropertyMappings(predicateURI);
			if(propertyMappings == null) {
				logger.warn("no property mappings defined for predicate: " + predicateURI);
			} else if (propertyMappings.size() > 1) {
				logger.warn("multiple property mappings defined for predicate: " + predicateURI);
			}
			AbstractPropertyMapping propertyMapping = propertyMappings.iterator().next();
			if(propertyMapping instanceof R2RMLPredicateObjectMap) {
				R2RMLPredicateObjectMap pom = (R2RMLPredicateObjectMap) propertyMapping;
				R2RMLObjectMap om = pom.getObjectMap();
				int mappingHashCode = -1;
				if(om != null) {
					mappingHashCode = om.hashCode();
					this.getOwner().getMapHashCodeMapping().put(mappingHashCode, om);
				} else {
					R2RMLRefObjectMap rom = pom.getRefObjectMap();
					if(rom != null) {
						mappingHashCode = rom.hashCode();
						this.getOwner().getMapHashCodeMapping().put(mappingHashCode, rom);
					}
				}
				
				if(mappingHashCode != -1) {
					ZConstant mappingHashCodeConstant = new ZConstant(
							mappingHashCode + "", ZConstant.NUMBER);
					SQLSelectItem mappingSelectItem = new SQLSelectItem();
					mappingSelectItem.setExpression(mappingHashCodeConstant);
					String mappingSelectItemAlias = constants.PREFIX_MAPPING_ID() + object.getName();
					mappingSelectItem.setAlias(mappingSelectItemAlias);
					mappingSelectItem.setDbType(this.getOwner().getDatabaseType());
					mappingSelectItem.setColumnType(constants.COLUMN_TYPE_INTEGER());
					childResult.add(mappingSelectItem);
					result.addAll(childResult);					
				}
			}
		}
		
		return result;
	}
}
