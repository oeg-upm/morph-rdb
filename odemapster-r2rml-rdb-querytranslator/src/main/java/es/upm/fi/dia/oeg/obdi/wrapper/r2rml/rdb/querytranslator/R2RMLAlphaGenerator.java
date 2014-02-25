package es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.querytranslator;

import java.sql.Connection;
import java.util.Collection;
import java.util.List;
import java.util.Vector;

import org.apache.log4j.Logger;

import Zql.ZExpression;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.vocabulary.RDF;

import es.upm.fi.dia.oeg.morph.base.Constants;
import es.upm.fi.dia.oeg.obdi.core.DBUtility;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractPropertyMapping;
import es.upm.fi.dia.oeg.obdi.core.querytranslator.AbstractAlphaGenerator;
import es.upm.fi.dia.oeg.obdi.core.querytranslator.AlphaResult;
import es.upm.fi.dia.oeg.obdi.core.querytranslator.QueryTranslationException;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLFromItem;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLFromItem.LogicalTableType;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLJoinTable;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLLogicalTable;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLQuery;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.R2RMLUtility;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.engine.R2RMLElementUnfoldVisitor;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLJoinCondition;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLLogicalTable;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLPredicateObjectMap;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLRefObjectMap;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTriplesMap;

public class R2RMLAlphaGenerator extends AbstractAlphaGenerator {
	private static Logger logger = Logger.getLogger(R2RMLAlphaGenerator.class);
	private Constants constants = new Constants();

	@Override
	protected SQLJoinTable calculateAlphaPredicateObject  (Triple triple
			, AbstractConceptMapping abstractConceptMapping
			, AbstractPropertyMapping abstractPropertyMapping
			, String logicalTableAlias) throws QueryTranslationException {
		SQLJoinTable result = null;
		
		R2RMLPredicateObjectMap pm = (R2RMLPredicateObjectMap) abstractPropertyMapping;  
		R2RMLRefObjectMap refObjectMap = pm.getRefObjectMap();
		
		if(refObjectMap != null) { 
			R2RMLLogicalTable parentLogicalTable = refObjectMap.getParentLogicalTable();
			if(parentLogicalTable == null) {
				String errorMessage = "Parent logical table is not found for RefObjectMap : " + refObjectMap;
				throw new QueryTranslationException(errorMessage);
			}
			R2RMLElementUnfoldVisitor unfolder = (R2RMLElementUnfoldVisitor) this.owner.getUnfolder();
			SQLLogicalTable sqlParentLogicalTableAux = unfolder.visit(parentLogicalTable);
			SQLJoinTable sqlParentLogicalTable = new SQLJoinTable(
					sqlParentLogicalTableAux, constants.JOINS_TYPE_INNER(), null); 
			String joinQueryAlias = R2RMLQueryTranslator.mapTripleAlias.get(triple);
			if(joinQueryAlias == null) {
				joinQueryAlias = sqlParentLogicalTableAux.generateAlias();
				R2RMLQueryTranslator.mapTripleAlias.put(triple, joinQueryAlias);
			}
			sqlParentLogicalTableAux.setAlias(joinQueryAlias);

			Collection<R2RMLJoinCondition> joinConditions = refObjectMap.getJoinConditions();
			String databaseType = this.owner.getDatabaseType();
			ZExpression onExpression = R2RMLUtility.generateJoinCondition(
					joinConditions, logicalTableAlias, joinQueryAlias
					, databaseType);
			if(onExpression != null) {
				sqlParentLogicalTable.setOnExpression(onExpression);
			}
			
			result = sqlParentLogicalTable;
		} 
		
		return result;
	}

	@Override
	protected SQLLogicalTable calculateAlphaPredicateObject2 (Triple triple
			, AbstractConceptMapping abstractConceptMapping
			, AbstractPropertyMapping abstractPropertyMapping
			, String logicalTableAlias) throws QueryTranslationException {
		SQLLogicalTable result = null;
		
		R2RMLPredicateObjectMap pm = (R2RMLPredicateObjectMap) abstractPropertyMapping;  
		R2RMLRefObjectMap refObjectMap = pm.getRefObjectMap();
		
		if(refObjectMap != null) { 
			R2RMLLogicalTable parentLogicalTable = refObjectMap.getParentLogicalTable();
			if(parentLogicalTable == null) {
				String errorMessage = "Parent logical table is not found for RefObjectMap : " + refObjectMap;
				throw new QueryTranslationException(errorMessage);
			}
			R2RMLElementUnfoldVisitor unfolder = (R2RMLElementUnfoldVisitor) this.owner.getUnfolder();
			SQLLogicalTable sqlParentLogicalTableAux = unfolder.visit(parentLogicalTable);
			result = sqlParentLogicalTableAux;
		} 
		
		return result;
	}

	@Override
	protected SQLLogicalTable calculateAlphaSubject(Node subject,
			AbstractConceptMapping abstractConceptMapping) throws QueryTranslationException {
		R2RMLTriplesMap cm = (R2RMLTriplesMap) abstractConceptMapping;
		R2RMLLogicalTable r2rmlLogicalTable = cm.getLogicalTable();
		//SQLLogicalTable sqlLogicalTable = new R2RMLElementUnfoldVisitor().visit(logicalTable);
		R2RMLElementUnfoldVisitor unfolder = (R2RMLElementUnfoldVisitor) this.owner.getUnfolder();
		SQLLogicalTable sqlLogicalTable = unfolder.visit(r2rmlLogicalTable);
		if(this.subqueryAsView && sqlLogicalTable instanceof SQLQuery) {
			try {
				Connection conn = this.owner.getConnection();
				
				String subQueryViewName = "sa" + Math.abs(subject.hashCode());
				String dropViewSQL = "DROP VIEW IF EXISTS " + subQueryViewName;
				logger.info(dropViewSQL + ";\n");
				DBUtility.execute(conn, dropViewSQL);
				
				String createViewSQL = "CREATE VIEW " + subQueryViewName + " AS " + sqlLogicalTable;
				logger.info(createViewSQL + ";\n");
				DBUtility.execute(conn, createViewSQL);
				
				sqlLogicalTable = new SQLFromItem(subQueryViewName, LogicalTableType.TABLE_NAME);				
			} catch(Exception e) {
				throw new QueryTranslationException(e);
			}
		}
		
		//String logicalTableAlias = sqlLogicalTable.generateAlias();
		String logicalTableAlias = cm.getLogicalTable().getAlias();
		if(logicalTableAlias == null || logicalTableAlias.equals("")) {
			logicalTableAlias = sqlLogicalTable.generateAlias();
		}
		//r2rmlLogicalTable.setAlias(logicalTableAlias);
		sqlLogicalTable.setAlias(logicalTableAlias);
//		cm.setAlias(logicalTableAlias);
		return sqlLogicalTable;
	}



	public List<SQLJoinTable> calculateAlphaPredicateObjectSTG(Triple tp
			, AbstractConceptMapping cm, String tpPredicateURI
			, String logicalTableAlias) throws Exception {
		List<SQLJoinTable> alphaPredicateObjects = new Vector<SQLJoinTable>();
		
		boolean isRDFTypeStatement = RDF.type.getURI().equals(tpPredicateURI);
		if(this.ignoreRDFTypeStatement && isRDFTypeStatement) {
			//do nothing
		} else {
			tp.getObject();
			Collection<AbstractPropertyMapping> pms = cm.getPropertyMappings(tpPredicateURI);
			if(pms != null && !pms.isEmpty()) {
				R2RMLPredicateObjectMap pm = (R2RMLPredicateObjectMap) pms.iterator().next();
				logger.debug("pm = " + pm);
				R2RMLRefObjectMap refObjectMap = pm.getRefObjectMap();
				if(refObjectMap != null) { 
					SQLJoinTable alphaPredicateObject = 
							this.calculateAlphaPredicateObject(tp, cm, pm, logicalTableAlias);
					alphaPredicateObjects.add(alphaPredicateObject);
				}
			} else {
				if(!isRDFTypeStatement) {
					String errorMessage = "Undefined mapping for : " + tpPredicateURI + " in : " + cm.toString();
					throw new QueryTranslationException(errorMessage);				
					
				}  
			}
		}
		return alphaPredicateObjects;
	}

	public List<SQLLogicalTable> calculateAlphaPredicateObjectSTG2(Triple tp
			, AbstractConceptMapping cm, String tpPredicateURI
			, String logicalTableAlias) throws Exception {
		List<SQLLogicalTable> alphaPredicateObjects = new Vector<SQLLogicalTable>();
		
		boolean isRDFTypeStatement = RDF.type.getURI().equals(tpPredicateURI);
		if(this.ignoreRDFTypeStatement && isRDFTypeStatement) {
			//do nothing
		} else {
			tp.getObject();
			Collection<AbstractPropertyMapping> pms = cm.getPropertyMappings(tpPredicateURI);
			if(pms != null && !pms.isEmpty()) {
				R2RMLPredicateObjectMap pm = (R2RMLPredicateObjectMap) pms.iterator().next();
				logger.debug("pm = " + pm);
				R2RMLRefObjectMap refObjectMap = pm.getRefObjectMap();
				if(refObjectMap != null) { 
					SQLLogicalTable alphaPredicateObject = 
							this.calculateAlphaPredicateObject2(tp, cm, pm, logicalTableAlias);
					alphaPredicateObjects.add(alphaPredicateObject);
				}
			} else {
				if(!isRDFTypeStatement) {
					String errorMessage = "Undefined mapping for : " + tpPredicateURI + " in : " + cm.toString();
					throw new QueryTranslationException(errorMessage);				
					
				}
			}
		}
		return alphaPredicateObjects;
	}
	
	@Override
	public AlphaResult calculateAlpha(Triple tp, AbstractConceptMapping abstractConceptMapping
			, String predicateURI)
			throws QueryTranslationException {
		AlphaResult alphaResult = null;
		Collection<AbstractPropertyMapping> pms = abstractConceptMapping.getPropertyMappings(predicateURI);
		if(pms != null && !pms.isEmpty()) {
			//alpha subject
			Node tpSubject = tp.getSubject();
			SQLLogicalTable alphaSubject = this.calculateAlphaSubject(tpSubject, abstractConceptMapping);
			String logicalTableAlias = alphaSubject.getAlias();
			
			//alpha predicate object
			List<SQLJoinTable> alphaPredicateObjects = new Vector<SQLJoinTable>();
			List<SQLLogicalTable> alphaPredicateObjects2 = new Vector<SQLLogicalTable>();
			if(pms != null && !pms.isEmpty()) {
				if(pms.size() > 1) {
					String errorMessage = "Multiple mappings of a predicate is not supported.";
					throw new QueryTranslationException(errorMessage);				
				}
				
				R2RMLPredicateObjectMap pm = (R2RMLPredicateObjectMap) pms.iterator().next();
				R2RMLRefObjectMap refObjectMap = pm.getRefObjectMap();
				if(refObjectMap != null) { 
					SQLJoinTable alphaPredicateObject = this.calculateAlphaPredicateObject(
							tp, abstractConceptMapping, pm, logicalTableAlias);
					alphaPredicateObjects.add(alphaPredicateObject);

					SQLLogicalTable alphaPredicateObject2 = this.calculateAlphaPredicateObject2(
							tp, abstractConceptMapping, pm, logicalTableAlias);
					alphaPredicateObjects2.add(alphaPredicateObject2);
				}
			}
			alphaResult = new AlphaResult(alphaSubject, alphaPredicateObjects, predicateURI);
			alphaResult.setAlphaPredicateObjects2(alphaPredicateObjects2);
			
			logger.debug("calculateAlpha = " + alphaResult);			
		}

		return alphaResult;
	}

	@Override
	public AlphaResult calculateAlpha(Triple tp,
			AbstractConceptMapping abstractConceptMapping, String predicateURI,
			AbstractPropertyMapping pm) throws QueryTranslationException {
		// TODO Auto-generated method stub
		return null;
	}

}
 