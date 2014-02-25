package es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.querytranslator;

import java.sql.Connection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.Logger;

import Zql.ZConstant;
import Zql.ZExpression;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;

import es.upm.fi.dia.oeg.morph.base.ColumnMetaData;
import es.upm.fi.dia.oeg.morph.base.Constants;
import es.upm.fi.dia.oeg.obdi.core.exception.InsatisfiableSQLExpression;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractLogicalTable;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractPropertyMapping;
import es.upm.fi.dia.oeg.obdi.core.querytranslator.AbstractBetaGenerator;
import es.upm.fi.dia.oeg.obdi.core.querytranslator.AbstractCondSQLGenerator;
import es.upm.fi.dia.oeg.obdi.core.querytranslator.AbstractQueryTranslator;
import es.upm.fi.dia.oeg.obdi.core.querytranslator.AlphaResult;
import es.upm.fi.dia.oeg.obdi.core.querytranslator.QueryTranslationException;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLDataType;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLSelectItem;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLUtility;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLLogicalTable;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLObjectMap;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLPredicateObjectMap;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLRefObjectMap;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTermMap;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTermMap.TermMapType;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTriplesMap;

public class R2RMLCondSQLGenerator extends AbstractCondSQLGenerator {
	private static Logger logger = Logger.getLogger(R2RMLCondSQLGenerator.class);
	private Constants constants = new Constants();
	
	public R2RMLCondSQLGenerator(AbstractQueryTranslator owner) {
		super(owner);
	}

	@Override
	protected ZExpression genCondSQLPredicateObject(Triple tp
			, AlphaResult alphaResult, AbstractBetaGenerator betaGenerator
			, AbstractConceptMapping cm, AbstractPropertyMapping pm) 
					throws QueryTranslationException, InsatisfiableSQLExpression {
		Node tpObject = tp.getObject();
		String logicalTableAlias = alphaResult.getAlphaSubject().getAlias();
		ZExpression result2 = null;
		R2RMLPredicateObjectMap poMap = (R2RMLPredicateObjectMap) pm;

		AbstractLogicalTable logicalTable = cm.getLogicalTable();
		Map<String, ColumnMetaData> columnsMetaData = logicalTable.getColumnsMetaData();

		Connection conn = this.owner.getConnection();
		if(columnsMetaData == null && conn != null) {
			try {
				logicalTable.buildMetaData(conn);
				columnsMetaData = logicalTable.getColumnsMetaData();
			} catch(Exception e) {
				throw new QueryTranslationException(e.getMessage());
			}
		}
		
		R2RMLRefObjectMap refObjectMap = poMap.getRefObjectMap();
		R2RMLObjectMap objectMap = poMap.getObjectMap();

		if(tpObject.isLiteral()) {
			Object objectLiteralValue = tpObject.getLiteral().getValue();

			if(objectMap != null) {
				String columnName = objectMap.getColumnName();
				if(columnName != null) {
					String columnNameWithAlias = columnName;
					if(logicalTableAlias != null && !logicalTableAlias.equals("")) {
						columnNameWithAlias = logicalTableAlias + "." + columnName;
					}
					ZConstant columnConstant = new ZConstant(columnNameWithAlias,  ZConstant.COLUMNNAME);
					ZConstant objectLiteral = new ZConstant(objectLiteralValue.toString(), ZConstant.STRING);
					result2 = new ZExpression("=", columnConstant, objectLiteral);
				}
			}
			if(refObjectMap != null) {
				String errorMessage = "triple.object is a literal, but RefObjectMap is specified instead of ObjectMap";
				logger.error(errorMessage);
				throw new InsatisfiableSQLExpression(errorMessage);
			}
		} else if(tpObject.isURI()) {
			if(objectMap != null && refObjectMap == null) {
				String uri = tpObject.getURI();
				TermMapType termMapType = objectMap.getTermMapType();
				if(termMapType == TermMapType.TEMPLATE) {
					result2 = this.generateCondForWellDefinedURI(objectMap
							, uri, logicalTableAlias, columnsMetaData);
				} else if(termMapType == TermMapType.COLUMN) {
					String columnName = objectMap.getColumnName();
					String columnNameWithAlias = columnName;
					if(logicalTableAlias != null) {
						columnNameWithAlias = logicalTableAlias + "." + columnName;
					}
					ZConstant zConstantObjectColumn = new ZConstant(columnNameWithAlias,  ZConstant.COLUMNNAME);
					ZConstant zConstantObjectURI = new ZConstant(uri.toString(), ZConstant.STRING);
					result2 = new ZExpression("=", zConstantObjectColumn, zConstantObjectURI);
				} else if(termMapType == TermMapType.CONSTANT) {
					//TODO
				}					
			} else if(tpObject.isVariable()) {
				
			}

			if(refObjectMap != null && objectMap == null) {
				//String refObjectMapAlias = refObjectMap.getAlias();
				String refObjectMapAlias = R2RMLQueryTranslator.mapTripleAlias.get(tp);

				//Collection<R2RMLJoinCondition> joinConditions = refObjectMap.getJoinConditions();
				//ZExp onExpression = R2RMLUtility.generateJoinCondition(joinConditions, logicalTableAlias, refObjectMapAlias);
				// onExpression done in alpha generator
				ZExpression onExpression = null; 

				ZExpression uriCondition = null;
				R2RMLTriplesMap parentTriplesMap = 
						refObjectMap.getParentTriplesMap();
				uriCondition = this.generateCondForWellDefinedURI(
						parentTriplesMap.getSubjectMap(), tpObject.getURI(),
						refObjectMapAlias, columnsMetaData);

				result2 = (ZExpression) SQLUtility.combineExpressions(
						onExpression, uriCondition, constants.SQL_LOGICAL_OPERATOR_AND());				
			}

			if(objectMap != null && refObjectMap != null) {
				String errorMessage = "Wrong mapping, ObjectMap and RefObjectMap shouldn't be specified at the same time.";
				throw new QueryTranslationException(errorMessage);
			}
		}
		return result2;
	}

	protected ZExpression generateCondForWellDefinedURI(R2RMLTermMap termMap
			, String uri, String alias, Map<String, ColumnMetaData> columnsMetaData) 
					throws InsatisfiableSQLExpression {
		ZExpression result = null;

		if(termMap.getTermMapType() == TermMapType.TEMPLATE) {
			Map<String, String> matchedColValues = termMap.getTemplateValues(uri);
			if(matchedColValues == null || matchedColValues.size() == 0) {
				String errorMessage = "uri " + uri + " doesn't match the template : " + termMap.getTemplateString();
				logger.debug(errorMessage);
				throw new InsatisfiableSQLExpression(errorMessage);	
			} else {
				Collection<ZExpression> exprs = new Vector<ZExpression>();
				for(String pkColumnString : matchedColValues.keySet()) {
					String value = matchedColValues.get(pkColumnString);
					String dbType = this.owner.getDatabaseType();
					SQLSelectItem pkColumnSelectItem = SQLSelectItem.createSQLItem(
							dbType, pkColumnString, alias);
					String columnTypeName = termMap.getColumnTypeName();
					if(columnTypeName == null && columnsMetaData != null) {
						if(columnsMetaData.get(pkColumnString) != null) {
							columnTypeName = columnsMetaData.get(pkColumnString).getDataType();
							termMap.setColumnTypeName(columnTypeName);						
						}
					}

					ZConstant pkColumnConstant = new ZConstant(
							pkColumnSelectItem.toString(), ZConstant.COLUMNNAME);
					ZConstant pkValueConstant = new ZConstant(value, ZConstant.STRING);
					if(columnTypeName != null) {
						if(Arrays.asList(SQLDataType.datatypeNumber).contains(columnTypeName)) {
							pkValueConstant = new ZConstant(value, ZConstant.NUMBER);
						} else if(Arrays.asList(SQLDataType.datatypeString).contains(columnTypeName)) {
							pkValueConstant = new ZConstant(value, ZConstant.STRING);
						}					
					}
					ZExpression expr = new ZExpression("=", pkColumnConstant, pkValueConstant);
					exprs.add(expr);
				}

				result = SQLUtility.combineExpresions(
						exprs, constants.SQL_LOGICAL_OPERATOR_AND());				
			}
		}

		logger.debug("generateCondForWellDefinedURI = " + result);
		return result;
	}

	@Override
	protected ZExpression genCondSQLSubjectURI(Node tpSubject,
			AlphaResult alphaResult, AbstractConceptMapping cm) 
					throws QueryTranslationException {
		ZExpression result2 = null;
		String subjectURI = tpSubject.getURI();
		R2RMLTriplesMap tm = (R2RMLTriplesMap) cm;

		ZConstant subjectURIConstant = new ZConstant(subjectURI, ZConstant.STRING);

		String logicalTableAlias = alphaResult.getAlphaSubject().getAlias();

		TermMapType subjectTermMapType = tm.getSubjectMap().getTermMapType();
		if(subjectTermMapType == TermMapType.TEMPLATE) {
			try {
				R2RMLLogicalTable logicalTable = ((R2RMLTriplesMap) cm).getLogicalTable();
				//ResultSetMetaData rsmd = logicalTable.getRsmd();
				Map<String, ColumnMetaData> columnsMetaData = logicalTable.getColumnsMetaData();

				if(columnsMetaData == null) {
					Connection conn = this.owner.getConnection();
					logicalTable.buildMetaData(conn);
					columnsMetaData = logicalTable.getColumnsMetaData();
				}

				result2 = this.generateCondForWellDefinedURI(tm.getSubjectMap(), 
						tpSubject.getURI(), logicalTableAlias, columnsMetaData);					
			} catch(Exception e) {
				throw new QueryTranslationException(e);
			}
		} else if(subjectTermMapType == TermMapType.COLUMN){
			ZConstant subjectMapColumn = new ZConstant(tm.getSubjectMap().getColumnName(), ZConstant.COLUMNNAME);
			result2 = new ZExpression("=", subjectMapColumn, subjectURIConstant);
		} else { //subjectTermMapType == TermMapType.CONSTANT
			ZConstant subjectMapColumn = new ZConstant(tm.getSubjectMap().getConstantValue(), ZConstant.COLUMNNAME);
			result2 = new ZExpression("=", subjectMapColumn, subjectURIConstant);
		}

		return result2;
	}
}
