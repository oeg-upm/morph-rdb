package es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.querytranslator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.log4j.Logger;

import Zql.ZConstant;
import Zql.ZSelectItem;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractPropertyMapping;
import es.upm.fi.dia.oeg.obdi.core.querytranslator.AbstractBetaGenerator;
import es.upm.fi.dia.oeg.obdi.core.querytranslator.AbstractQueryTranslator;
import es.upm.fi.dia.oeg.obdi.core.querytranslator.AlphaResult;
import es.upm.fi.dia.oeg.obdi.core.querytranslator.QueryTranslationException;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLSelectItem;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.R2RMLUtility;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLObjectMap;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLPredicateObjectMap;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLRefObjectMap;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLSubjectMap;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTermMap.TermMapType;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTriplesMap;

public class R2RMLBetaGenerator extends AbstractBetaGenerator {
	private static Logger logger = Logger.getLogger(R2RMLBetaGenerator.class);

	public R2RMLBetaGenerator(AbstractQueryTranslator owner) {
		super(owner);
	}


	@Override
	public List<ZSelectItem> calculateBetaObject(Triple tp
			, AbstractConceptMapping cm, String predicateURI
			, AlphaResult alphaResult) throws QueryTranslationException {
		List<ZSelectItem> result = new ArrayList<ZSelectItem>();
		
		Node object = tp.getObject();
		
		//String logicalTableAlias = triplesMap.getLogicalTable().getAlias();
		String logicalTableAlias = alphaResult.getAlphaSubject().getAlias();
		
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
			R2RMLPredicateObjectMap predicateObjectMap =(R2RMLPredicateObjectMap) pm;
			R2RMLRefObjectMap refObjectMap = predicateObjectMap.getRefObjectMap(); 

			if(refObjectMap == null) {
				R2RMLObjectMap objectMap = predicateObjectMap.getObjectMap();
//				if(object.isVariable()) {
//					this.getOwner().getMapVarMapping2().put(
//							object.getName(), objectMap);
//				}

				if(objectMap.getTermMapType() == TermMapType.CONSTANT) {
					String constantValue = objectMap.getConstantValue();
					SQLSelectItem selectItem = new SQLSelectItem();
					ZConstant zConstant = new ZConstant(constantValue, ZConstant.STRING);
					selectItem.setExpression(zConstant);
					result.add(selectItem);
				} else {
					Collection<String> databaseColumnsString = objectMap.getDatabaseColumnsString();
					for(String databaseColumnString : databaseColumnsString) {
						String alphaSubjectAlias = alphaResult.getAlphaSubject().getAlias();
						if(alphaSubjectAlias != null) {
							databaseColumnString = alphaSubjectAlias + "." + databaseColumnString;  
						}

						SQLSelectItem selectItem = R2RMLUtility.toSelectItem(databaseColumnString
								, logicalTableAlias, this.owner.getDatabaseType());
						result.add(selectItem);
					}
				}
			} else {
//				if(object.isVariable()) {
//					this.getOwner().getMapVarMapping2().put(object.getName(), refObjectMap);
//				}
				
				List<String> databaseColumnsString = refObjectMap.getParentDatabaseColumnsString();
				//String refObjectMapAlias = refObjectMap.getAlias(); 
				String refObjectMapAlias = R2RMLQueryTranslator.mapTripleAlias.get(tp);

				if(databaseColumnsString != null) {
					for(String databaseColumnString : databaseColumnsString) {
						String alphaSubjectAlias = alphaResult.getAlphaSubject().getAlias();
						if(alphaSubjectAlias != null) {
							databaseColumnString = alphaSubjectAlias + "." + databaseColumnString;  
						}
						SQLSelectItem selectItem = R2RMLUtility.toSelectItem(databaseColumnString
								, refObjectMapAlias, this.owner.getDatabaseType());
						result.add(selectItem);
					}
				}
			}			
		}

		return result;
	}


	@Override
	public List<ZSelectItem> calculateBetaSubject(
			AbstractConceptMapping cm, AlphaResult alphaResult) {
		List<ZSelectItem> result = new ArrayList<ZSelectItem>();
		R2RMLTriplesMap triplesMap = (R2RMLTriplesMap) cm;
		R2RMLSubjectMap subjectMap = triplesMap.getSubjectMap();
		
		

		
		//String logicalTableAlias = triplesMap.getLogicalTable().getAlias();
		String logicalTableAlias = alphaResult.getAlphaSubject().getAlias();
		
		Collection<String> databaseColumnsString = 
				subjectMap.getDatabaseColumnsString();
		if(databaseColumnsString != null) {
			for(String databaseColumnString : databaseColumnsString) {
				String alphaSubjectAlias = alphaResult.getAlphaSubject().getAlias();
				if(alphaSubjectAlias != null) {
					databaseColumnString = alphaSubjectAlias + "." + databaseColumnString;  
				}
				
				SQLSelectItem selectItem = R2RMLUtility.toSelectItem(databaseColumnString
						, logicalTableAlias, this.owner.getDatabaseType());
				result.add(selectItem);
			}
		}

		
		return result;
	}

	protected AbstractQueryTranslator getOwner() {
		AbstractQueryTranslator result = super.getOwner();
		return result;
	}



}
