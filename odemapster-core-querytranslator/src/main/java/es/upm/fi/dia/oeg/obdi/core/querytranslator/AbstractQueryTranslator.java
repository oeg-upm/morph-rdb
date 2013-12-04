package es.upm.fi.dia.oeg.obdi.core.querytranslator;

import java.sql.Connection;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.Logger;

import Zql.ZConstant;
import Zql.ZExp;
import Zql.ZExpression;
import Zql.ZGroupBy;
import Zql.ZOrderBy;
import Zql.ZSelectItem;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.SortCondition;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct;
import com.hp.hpl.jena.sparql.algebra.op.OpExtend;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpGroup;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpOrder;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.algebra.op.OpSlice;
import com.hp.hpl.jena.sparql.algebra.op.OpUnion;
import com.hp.hpl.jena.sparql.algebra.optimize.Optimize;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.core.VarExprList;
import com.hp.hpl.jena.sparql.expr.E_Bound;
import com.hp.hpl.jena.sparql.expr.E_Function;
import com.hp.hpl.jena.sparql.expr.E_LogicalAnd;
import com.hp.hpl.jena.sparql.expr.E_LogicalNot;
import com.hp.hpl.jena.sparql.expr.E_LogicalOr;
import com.hp.hpl.jena.sparql.expr.E_NotEquals;
import com.hp.hpl.jena.sparql.expr.E_OneOf;
import com.hp.hpl.jena.sparql.expr.E_Regex;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprAggregator;
import com.hp.hpl.jena.sparql.expr.ExprFunction;
import com.hp.hpl.jena.sparql.expr.ExprFunction1;
import com.hp.hpl.jena.sparql.expr.ExprFunction2;
import com.hp.hpl.jena.sparql.expr.ExprList;
import com.hp.hpl.jena.sparql.expr.NodeValue;
import com.hp.hpl.jena.sparql.expr.aggregate.AggAvg;
import com.hp.hpl.jena.sparql.expr.aggregate.AggCount;
import com.hp.hpl.jena.sparql.expr.aggregate.AggMax;
import com.hp.hpl.jena.sparql.expr.aggregate.AggMin;
import com.hp.hpl.jena.sparql.expr.aggregate.AggSum;
import com.hp.hpl.jena.sparql.expr.aggregate.Aggregator;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.XSD;

import es.upm.fi.dia.oeg.morph.base.ColumnMetaData;
import es.upm.fi.dia.oeg.morph.base.Constants;
import es.upm.fi.dia.oeg.morph.base.MorphSQLUtility;
import es.upm.fi.dia.oeg.morph.base.SPARQLUtility;
import es.upm.fi.dia.oeg.morph.base.TriplePatternPredicateBounder;
import es.upm.fi.dia.oeg.morph.querytranslator.MorphQueryRewriter;
import es.upm.fi.dia.oeg.morph.querytranslator.MorphSQLSelectItemGenerator;
import es.upm.fi.dia.oeg.morph.querytranslator.NameGenerator;
import es.upm.fi.dia.oeg.obdi.core.ConfigurationProperties;
import es.upm.fi.dia.oeg.obdi.core.DBUtility;
import es.upm.fi.dia.oeg.obdi.core.engine.AbstractResultSet;
import es.upm.fi.dia.oeg.obdi.core.engine.AbstractUnfolder;
import es.upm.fi.dia.oeg.obdi.core.engine.IQueryTranslationOptimizer;
import es.upm.fi.dia.oeg.obdi.core.engine.IQueryTranslator;
import es.upm.fi.dia.oeg.obdi.core.exception.InsatisfiableSQLExpression;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractPropertyMapping;
import es.upm.fi.dia.oeg.obdi.core.sql.IQuery;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLFromItem;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLFromItem.LogicalTableType;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLJoinTable;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLLogicalTable;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLQuery;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLUnion;
//import es.upm.fi.dia.oeg.obdi.core.sql.SQLUtility;
import es.upm.fi.dia.oeg.upm.morph.sql.MorphSQLConstant;
import es.upm.fi.dia.oeg.upm.morph.sql.MorphSQLSelectItem;

public abstract class AbstractQueryTranslator implements IQueryTranslator {
	private static Logger logger = Logger.getLogger(AbstractQueryTranslator.class);

	protected Query sparqQuery = null;
	
	private Connection connection;
	//protected String queryFilePath;
	protected AbstractMappingDocument mappingDocument;
	protected AbstractUnfolder unfolder;
	private ConfigurationProperties configurationProperties;
	protected String databaseType;
	
	//query translator
	public enum POS {sub, pre, obj}
	protected Map<Node, Set<AbstractConceptMapping>> mapInferredTypes;
	protected IQueryTranslationOptimizer optimizer = null;
	protected boolean ignoreRDFTypeStatement = false;
//	protected Map<Op, Collection<Node>> mapTermsC = new HashMap<Op, Collection<Node>>();
//	protected Map<Op, String> mapTransGP1Alias = new HashMap<Op, String>();
	//private Map<String, Object> mapVarMapping2 = new HashMap<String, Object>();
	private Map<Integer, Object> mapHashCodeMapping = new HashMap<Integer, Object>();
	private Map<String, ZSelectItem> mapAggreatorAlias = new HashMap<String, ZSelectItem>();//varname - selectitem
	private Map<String, String> functionsMap = new HashMap<String, String>();
	Collection<String> notNullColumns = new Vector<String>();

	//chebotko functions
	private AbstractAlphaGenerator alphaGenerator;
	private AbstractBetaGenerator betaGenerator;
	private NameGenerator nameGenerator;
	private AbstractPRSQLGenerator prSQLGenerator;
	private AbstractCondSQLGenerator condSQLGenerator;

	public AbstractQueryTranslator() {
		this.nameGenerator = new NameGenerator();
		Optimize.setFactory(new QueryRewritterFactory());

		functionsMap.put(E_Bound.class.toString(), "IS NOT NULL");
		functionsMap.put(E_LogicalNot.class.toString(), "NOT");
		functionsMap.put(E_LogicalOr.class.toString(), "OR");
		functionsMap.put(E_LogicalAnd.class.toString(), "AND");
		functionsMap.put(E_Regex.class.toString(), "LIKE");
		functionsMap.put(E_OneOf.class.toString(), "IN");
	}


	protected abstract void buildAlphaGenerator();

	protected abstract void buildBetaGenerator();

	protected abstract void buildCondSQLGenerator();

	protected abstract void buildPRSQLGenerator();

	private Collection<ZSelectItem> generateMappingIdSelectItems(Collection<Node> nodes
			, Collection<ZSelectItem> selectItems, String prefix) {
		if(prefix == null) {
			prefix = "";
		} else {
			if(!prefix.endsWith(".")) {
				prefix += ".";
			}	
		}
		
		Collection<ZSelectItem> result = new Vector<ZSelectItem>();
		
		for(Node term : nodes) {
			if(term.isVariable()) {
				Collection<ZSelectItem> mappingsSelectItemsAux = 
						MorphSQLUtility.getSelectItemsMapPrefix(selectItems, term, prefix, this.databaseType);
				for(ZSelectItem mappingsSelectItemAux : mappingsSelectItemsAux) {
//					SQLSelectItem newSelectItem = new SQLSelectItem(prefix + mappingsSelectItemAux.getAlias());
//					newSelectItem.setDbType(this.databaseType);
					ZSelectItem newSelectItem = MorphSQLSelectItem.apply(
							mappingsSelectItemAux.getAlias(), prefix, this.databaseType, null);
					result.add(newSelectItem);
				}
			}
		}
		
		return result;
	}


	
	protected String generateTermCName(Node termC) {
		String termCName = this.getNameGenerator().generateName(termC);
		return termCName;		
	}

	public AbstractAlphaGenerator getAlphaGenerator() {
		return alphaGenerator;
	}

	public AbstractBetaGenerator getBetaGenerator() {
		return betaGenerator;
	}

	private Collection<String> getColumnsByNode(Node node, Collection<ZSelectItem> oldSelectItems) {
		Collection<String> result = new LinkedHashSet<String>();
		String nameSelectVar = nameGenerator.generateName(node);

		Iterator<ZSelectItem> oldSelectItemsIterator = oldSelectItems.iterator();
		int i=0;
		while(oldSelectItemsIterator.hasNext()) {
			ZSelectItem oldSelectItem = oldSelectItemsIterator.next(); 
			String oldAlias = oldSelectItem.getAlias();
			String selectItemName;
			if(oldAlias == null || oldAlias.equals("")) {
				selectItemName = oldSelectItem.getColumn();
			} else {
				selectItemName = oldAlias; 
			}

			
			if(selectItemName.equalsIgnoreCase(nameSelectVar)) {
				result.add(selectItemName);
			} else if (selectItemName.contains(nameSelectVar + "_")) {
				String selectItemNameParts = nameSelectVar + "_" + i;
				result.add(selectItemNameParts);
				i++;
			}
			
		}
		return result;
	}

	public AbstractCondSQLGenerator getCondSQLGenerator() {
		return condSQLGenerator;
	}

	public ConfigurationProperties getConfigurationProperties() {
		return configurationProperties;
	}

	public Connection getConnection() {
		return connection;
	}

	public String getDatabaseType() {
		return databaseType;
	}

//	public Map<Integer, Object> getMapHashCodeMapping() {
//		return mapHashCodeMapping;
//	}
	
	public Object getMappedMapping(Integer hashCode) {
		return this.mapHashCodeMapping.get(hashCode);
	}
	
	public void putMappedMapping(Integer key, Object value) {
		this.mapHashCodeMapping.put(key, value);
	}

	public AbstractMappingDocument getMappingDocument() {
		return mappingDocument;
	}

	public String getMappingDocumentURL() {
		return this.mappingDocument.getMappingDocumentPath();
	}

	public NameGenerator getNameGenerator() {
		return nameGenerator;
	}

	public IQueryTranslationOptimizer getOptimizer() {
		return optimizer;
	}

	public AbstractPRSQLGenerator getPrSQLGenerator() {
		return prSQLGenerator;
	}

	
	private Collection<ZSelectItem> getSelectItemsByNode(Node node, Collection<ZSelectItem> oldSelectItems) {
		Collection<ZSelectItem> result = new LinkedHashSet<ZSelectItem>();
		String nameSelectVar = nameGenerator.generateName(node);

		Iterator<ZSelectItem> oldSelectItemsIterator = oldSelectItems.iterator();
		while(oldSelectItemsIterator.hasNext()) {
			ZSelectItem oldSelectItem = oldSelectItemsIterator.next(); 
			String oldAlias = oldSelectItem.getAlias();
			String selectItemName;
			if(oldAlias == null || oldAlias.equals("")) {
				selectItemName = oldSelectItem.getColumn();
			} else {
				selectItemName = oldAlias; 
			}

			
			if(selectItemName.equalsIgnoreCase(nameSelectVar)) {
				result.add(oldSelectItem);
			} else if (selectItemName.contains(nameSelectVar + "_")) {
				result.add(oldSelectItem);
			}
			
		}
		return result;
	}

	public AbstractUnfolder getUnfolder() {
		return unfolder;
	}

	public boolean isIgnoreRDFTypeStatement() {
		return ignoreRDFTypeStatement;
	}

	public void setAlphaGenerator(AbstractAlphaGenerator alphaGenerator) {
		this.alphaGenerator = alphaGenerator;
	}

	public void setBetaGenerator(AbstractBetaGenerator betaGenerator) {
		this.betaGenerator = betaGenerator;
	}

	public void setCondSQLGenerator(AbstractCondSQLGenerator condSQLGenerator) {
		this.condSQLGenerator = condSQLGenerator;
	}

	public void setConfigurationProperties(
			ConfigurationProperties configurationProperties) {
		this.configurationProperties = configurationProperties;
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}

	public void setDatabaseType(String databaseType) {
		this.databaseType = databaseType;
	}


	public void setIgnoreRDFTypeStatement(boolean ignoreRDFTypeStatement) {
		this.ignoreRDFTypeStatement = ignoreRDFTypeStatement;
	}



	public void setMappingDocument(AbstractMappingDocument mappingDocument) {
		this.mappingDocument = mappingDocument;
	}

	public void setNameGenerator(NameGenerator nameGenerator) {
		this.nameGenerator = nameGenerator;
	}

	public void setOptimizer(IQueryTranslationOptimizer optimizer) {
		this.optimizer = optimizer;

	}



	public void setPrSQLGenerator(AbstractPRSQLGenerator prSQLGenerator) {
		this.prSQLGenerator = prSQLGenerator;
	}


	public void setUnfolder(AbstractUnfolder unfolder) {
		this.unfolder = unfolder;
	}





	protected IQuery trans(Op op) throws Exception {
		IQuery result = null;
		if(op instanceof OpBGP) { //triple or bgp pattern
			OpBGP bgp = (OpBGP) op;
			if(bgp.getPattern().size() == 1) {
				Triple tp = bgp.getPattern().get(0);
				result = this.trans(tp);
			} else {
				result = this.trans(bgp);	
			}
		} else if(op instanceof OpJoin) { // AND pattern
			OpJoin opJoin = (OpJoin) op;
			result = this.trans(opJoin);
		} else if(op instanceof OpLeftJoin) { //OPT pattern
			OpLeftJoin opLeftJoin = (OpLeftJoin) op;
			result = this.trans(opLeftJoin);
		} else if(op instanceof OpUnion) { //UNION pattern
			OpUnion opUnion = (OpUnion) op;
			result = this.trans(opUnion);
		} else if(op instanceof OpFilter) { //FILTER pattern
			OpFilter opFilter = (OpFilter) op;
			result = this.trans(opFilter);
		} else if(op instanceof OpProject) {
			OpProject opProject = (OpProject) op;
			result = this.trans(opProject);
		} else if(op instanceof OpSlice) {
			OpSlice opSlice = (OpSlice) op;
			result = this.trans(opSlice);
		} else if(op instanceof OpDistinct) {
			OpDistinct opDistinct = (OpDistinct) op;
			result = this.trans(opDistinct);
		} else if(op instanceof OpOrder) {
			OpOrder opOrder = (OpOrder) op;
			result = this.trans(opOrder);
		} else if(op instanceof OpExtend) {
			OpExtend opExtend = (OpExtend) op;
			result = this.trans(opExtend);
		} else if(op instanceof OpGroup) {
			OpGroup opGroup = (OpGroup) op;
			result = this.trans(opGroup);
		} else {
			throw new QueryTranslationException("Unsupported query!");
		}

		if(result != null) {
			result.setDatabaseType(databaseType);	
		}

		return result;
	}


	protected IQuery trans(OpBGP bgp) throws Exception {
		IQuery transBGPSQL = null;

		if(QueryTranslatorUtility.isTriplePattern(bgp)) { //triple pattern
			Triple tp = bgp.getPattern().getList().get(0);
			transBGPSQL = this.trans(tp);
		} else { //bgp pattern
			List<Triple> triples = bgp.getPattern().getList();
			boolean isSTG = QueryTranslatorUtility.isSTG(triples);

			if(this.optimizer != null && this.optimizer.isSelfJoinElimination() && isSTG) {
				transBGPSQL = this.transSTG(triples);
			} else {
				int separationIndex = 1;
				if(this.optimizer != null && this.optimizer.isSelfJoinElimination()) {
					separationIndex = QueryTranslatorUtility.getFirstTBEndIndex(triples);
				}
				List<Triple> gp1TripleList = triples.subList(0, separationIndex);
				OpBGP gp1 = new OpBGP(BasicPattern.wrap(gp1TripleList));
				List<Triple> gp2TripleList = triples.subList(separationIndex, triples.size());
				OpBGP gp2 = new OpBGP(BasicPattern.wrap(gp2TripleList));

				transBGPSQL = this.transJoin(bgp, gp1, gp2, Constants.JOINS_TYPE_INNER());
			}
		}

		return transBGPSQL;
	}

	protected IQuery trans(OpDistinct opDistinct) throws Exception {
		Op opDistinctSubOp = opDistinct.getSubOp(); 
		IQuery opDistinctSubOpSQL = this.trans(opDistinctSubOp);
		if(opDistinctSubOpSQL instanceof SQLQuery) {
			((SQLQuery)opDistinctSubOpSQL).setDistinct(true);	
		}
		return opDistinctSubOpSQL;
	}

	protected IQuery trans(OpExtend opExtend)  throws Exception {
		Op subOp = opExtend.getSubOp();
		IQuery subOpSQL = this.trans(subOp);
		Collection<ZSelectItem> selectItems = subOpSQL.getSelectItems();
		
		Map<Var, Expr> opExtendExprs = opExtend.getVarExprList().getExprs();
		for(Var var : opExtendExprs.keySet()) {
			Expr expr = opExtendExprs.get(var);
			String exprVarName = expr.getVarName();
			ZSelectItem selectItem = this.mapAggreatorAlias.get(exprVarName);
			String alias = this.nameGenerator.generateName(var);
			selectItem.setAlias(alias);
			
			String mapPrefixIdOldAlias = Constants.PREFIX_MAPPING_ID() 
					+ exprVarName.replaceAll("\\.", "dot_"); 
			String mapPrefixIdNewAlias = Constants.PREFIX_MAPPING_ID() + var.getName();
			Collection<ZSelectItem> mapPrefixIdSelectItems = 
					MorphSQLUtility.getSelectItemsByAlias(selectItems, mapPrefixIdOldAlias);
			ZSelectItem mapPrefixIdSelectItem = mapPrefixIdSelectItems.iterator().next();
			mapPrefixIdSelectItem.setAlias(mapPrefixIdNewAlias);
			
			expr.toString();
		}
		return subOpSQL;
	}

	protected IQuery trans(OpFilter opFilter) throws Exception {
		Op opFilterSubOp = opFilter.getSubOp();
		IQuery subOpSQL = this.trans(opFilterSubOp);
		String transGPSQLAlias = subOpSQL.generateAlias();
		Collection<ZSelectItem> subOpSelectItems = subOpSQL.getSelectItems(); 
		
		ExprList exprList = opFilter.getExprs();
		SQLFromItem resultFrom;
		if(this.optimizer != null && this.optimizer.isSubQueryAsView()) {
			Connection conn = this.getConnection();
			String subQueryViewName = "sqf" + Math.abs(opFilterSubOp.hashCode());
			String dropViewSQL = "DROP VIEW IF EXISTS " + subQueryViewName;
			logger.info(dropViewSQL + ";\n");
			DBUtility.execute(conn, dropViewSQL);
			String createViewSQL = "CREATE VIEW " + subQueryViewName + " AS " + subOpSQL;
			logger.info(createViewSQL + ";\n");
			DBUtility.execute(conn, createViewSQL);
			resultFrom = new SQLFromItem(subQueryViewName, LogicalTableType.TABLE_NAME, this.databaseType);
		} else {
			resultFrom = new SQLFromItem(subOpSQL.toString(), LogicalTableType.QUERY_STRING, this.databaseType);
		}
		resultFrom.setAlias(transGPSQLAlias);

		IQuery transFilterSQL = null;
		ZExpression exprListSQL = this.transExprList(
				opFilterSubOp, exprList, subOpSelectItems, subOpSQL.getAlias());
		if(this.optimizer != null && this.optimizer.isSubQueryElimination()) {
			subOpSQL.pushFilterDown(exprListSQL);
			transFilterSQL = subOpSQL;
		} else {
			Collection<ZSelectItem> oldSelectItems = subOpSQL.getSelectItems();
			Collection<ZSelectItem> newSelectItems = new Vector<ZSelectItem>();
			
			for(ZSelectItem oldSelectItem : oldSelectItems) {
				String oldSelectItemAlias = oldSelectItem.getAlias();
				String columnType = null;
//				if(oldSelectItem instanceof MorphSQLSelectItem) {
//					columnType = ((MorphSQLSelectItem) oldSelectItem).columnType();
//				}
				
				//ZSelectItem newSelectItem = MorphSQLSelectItem.apply(oldSelectItemAlias, transGPSQLAlias, databaseType, columnType);
				ZSelectItem newSelectItem = MorphSQLSelectItem.apply(
						oldSelectItemAlias, null, databaseType, columnType);
				
				newSelectItem.setAlias(oldSelectItemAlias);
				newSelectItems.add(newSelectItem);
			}
			//			ZSelectItem newSelectItem = new ZSelectItem("*");
			//			newSelectItems.add(newSelectItem);
			SQLQuery resultAux = new SQLQuery(subOpSQL);
			resultAux.setSelectItems(newSelectItems);
			resultAux.addWhere(exprListSQL);
			transFilterSQL = resultAux;
		}

		return transFilterSQL;
	}


	protected IQuery trans(OpGroup opGroup)  throws Exception {
		String dbType = this.databaseType;
		MorphSQLSelectItemGenerator selectItemGenerator = new MorphSQLSelectItemGenerator(
				this.nameGenerator, dbType);
		
		Op subOp = opGroup.getSubOp();
		IQuery subOpSQL = this.trans(subOp);
		String subOpSQLAlias = subOpSQL.getAlias();
		IQuery transOpGroup = subOpSQL;
		Collection<ZSelectItem> oldSelectItems = subOpSQL.getSelectItems();
		Collection<ZSelectItem> newSelectItems = new Vector<ZSelectItem>();
		Collection<ZSelectItem> mapPrefixSelectItems = new Vector<ZSelectItem>();

		VarExprList groupVars = opGroup.getGroupVars();
		List<Var> vars = groupVars.getVars();
		Vector<ZExp> groupByExps = new Vector<ZExp>();
		for(Var var : vars) {
			Collection<ZSelectItem> selectItemsByVars = this.getSelectItemsByNode(var, oldSelectItems);
			newSelectItems.addAll(selectItemsByVars);

			for(ZSelectItem selectItemByVar : selectItemsByVars) {
				String selectItemValue = MorphSQLUtility.getValueWithoutAlias(selectItemByVar);
				ZExp zExp = new ZConstant(selectItemValue, ZConstant.COLUMNNAME);
				groupByExps.add(zExp);
			}
			
			Collection<ZSelectItem> mapPrefixSelectItemsAux = MorphSQLUtility.getSelectItemsMapPrefix(
					oldSelectItems, var, subOpSQLAlias, this.databaseType);
			mapPrefixSelectItems.addAll(mapPrefixSelectItemsAux);
		}
		
		
		
		List<ExprAggregator> aggregators = opGroup.getAggregators();
		for(ExprAggregator exprAggregator : aggregators) {
			Aggregator aggregator = exprAggregator.getAggregator();
			String functionName;
			if(aggregator instanceof AggAvg) {
				functionName = Constants.AGGREGATION_FUNCTION_AVG();
			} else if(aggregator instanceof AggSum) {
				functionName = Constants.AGGREGATION_FUNCTION_SUM();
			} else if(aggregator instanceof AggCount) {
				functionName = Constants.AGGREGATION_FUNCTION_COUNT();
			} else if(aggregator instanceof AggMax) {
				functionName = Constants.AGGREGATION_FUNCTION_MAX();
			} else if(aggregator instanceof AggMin) {
				functionName = Constants.AGGREGATION_FUNCTION_MIN();
			} else {
				String errorMessage = "Unsupported aggregation function " + aggregator;
				logger.error(errorMessage);
				throw new Exception(errorMessage);
			}
			
			Set<Var> varsMentioned = aggregator.getExpr().getVarsMentioned();
			if(varsMentioned.size() > 1) {
				String errorMessage = "Multiple variables in aggregation function is not supported: " + aggregator;
				logger.error(errorMessage);
				throw new Exception(errorMessage);
			}
			
			Var var = varsMentioned.iterator().next();
			Var exprAggregatorVar = exprAggregator.getVar();
			String aggregatorVarName = exprAggregatorVar.getName();
			String aggregatorAlias = aggregatorVarName.replaceAll("\\.", "dot_");
			//Collection<ZSelectItem> aggregatedSelectItems = this.generateSelectItem(var, subOpSQLAlias, oldSelectItems, true);
//			Collection<ZSelectItem> aggregatedSelectItems = this.generateSelectItem(
//					var, subOpSQLAlias, oldSelectItems, true);
			Collection<ZSelectItem> aggregatedSelectItems = selectItemGenerator.generateSelectItem(
					var, subOpSQLAlias, oldSelectItems, true);
			
			if(aggregatedSelectItems.size() > 1) {
				String errorMessage = "Multiple columns in aggregation function is not supported: " + aggregatedSelectItems;
				logger.error(errorMessage);
				throw new Exception(errorMessage);				
			}
			
			transOpGroup.pushProjectionsDown(aggregatedSelectItems);
			Collection<ZSelectItem> pushedAggregatedSelectItems = transOpGroup.getSelectItems(); 
			ZSelectItem pushedAggregatedSelectItem = pushedAggregatedSelectItems.iterator().next();
			pushedAggregatedSelectItem.setAggregate(functionName);
			pushedAggregatedSelectItem.setAlias(Constants.PREFIX_VAR() + aggregatorAlias);
			newSelectItems.add(pushedAggregatedSelectItem);
			this.mapAggreatorAlias.put(aggregatorVarName, pushedAggregatedSelectItem);
			
			Collection<ZSelectItem> mapPrefixSelectItemsAux = MorphSQLUtility.getSelectItemsMapPrefix(
					oldSelectItems, var, subOpSQLAlias, this.databaseType);
			ZSelectItem mapPrefixSelectItemAux = mapPrefixSelectItemsAux.iterator().next();
			String mapPrefixSelectItemAuxAlias = Constants.PREFIX_MAPPING_ID() + aggregatorAlias;
			mapPrefixSelectItemAux.setAlias(mapPrefixSelectItemAuxAlias);;
			
			mapPrefixSelectItems.addAll(mapPrefixSelectItemsAux);
		}
		
		ZGroupBy zGroupBy = new ZGroupBy(groupByExps);
		transOpGroup.addGroupBy(zGroupBy);
		transOpGroup.setSelectItems(newSelectItems);
		transOpGroup.addSelectItems(mapPrefixSelectItems);
		
		return transOpGroup;
	}

	protected IQuery trans(OpJoin opJoin)  throws Exception {
		IQuery transJoinSQL = null;
		Op opLeft = opJoin.getLeft();
		Op opRight = opJoin.getRight();
		transJoinSQL = this.transJoin(opJoin, opLeft, opRight, Constants.JOINS_TYPE_INNER());
		return transJoinSQL;
	}

	protected IQuery trans(OpLeftJoin opLeftJoin) throws Exception {
		IQuery transLeftJoinSQL = null;
		Op opLeft = opLeftJoin.getLeft();
		Op opRight = opLeftJoin.getRight();

		transLeftJoinSQL = this.transJoin(opLeftJoin, opLeft, opRight, Constants.JOINS_TYPE_LEFT());
		return transLeftJoinSQL;
	}
	protected IQuery trans(OpOrder opOrder) throws Exception {
		Op opOrderSubOp = opOrder.getSubOp();
		IQuery opOrderSubOpSQL = this.trans(opOrderSubOp);

		Vector<ZOrderBy> orderByConditions = new Vector<ZOrderBy>();
		for(SortCondition sortCondition : opOrder.getConditions()) {
			int sortConditionDirection = sortCondition.getDirection();
			Expr sortConditionExpr = sortCondition.getExpression();
			Var sortConditionVar = sortConditionExpr.asVar();

			String nameSortConditionVar = nameGenerator.generateName(sortConditionVar);
			ZExp zExp = MorphSQLConstant.apply(nameSortConditionVar, ZConstant.COLUMNNAME, this.databaseType, null);
			//ZExp zExp = new ZConstant(sortConditionVar.getVarName(), ZConstant.COLUMNNAME);

			ZOrderBy zOrderBy = new ZOrderBy(zExp);
			if(sortConditionDirection == Query.ORDER_DEFAULT) {
				zOrderBy.setAscOrder(true);
			} else if(sortConditionDirection == Query.ORDER_ASCENDING) {
				zOrderBy.setAscOrder(true);
			} if(sortConditionDirection == Query.ORDER_DESCENDING) {
				zOrderBy.setAscOrder(false);
			} else {
				zOrderBy.setAscOrder(true);
			}
			orderByConditions.add(zOrderBy);
		}

		IQuery transOpOrder; 
		//		if(this.optimizer != null && this.optimizer.isSubQueryElimination()) {
		//			opOrderSubOpSQL.pushOrderByDown(orderByConditions);
		//			transOpOrder = opOrderSubOpSQL;
		//		} else {
		//			opOrderSubOpSQL.setOrderBy(orderByConditions);
		//			transOpOrder = opOrderSubOpSQL;
		//		}

		//always push order by, if not, the result is incorrect!
		opOrderSubOpSQL.setOrderBy(orderByConditions);
		transOpOrder = opOrderSubOpSQL;

		return transOpOrder;
	}


	protected IQuery trans(OpProject opProject) throws Exception {
		
		MorphSQLSelectItemGenerator selectItemGenerator = new MorphSQLSelectItemGenerator(
				this.nameGenerator, this.databaseType);
		Op opProjectSubOp = opProject.getSubOp();
		IQuery opProjectSubOpSQL = this.trans(opProjectSubOp);

		Collection<ZSelectItem> oldSelectItems = opProjectSubOpSQL.getSelectItems();
		//Collection<ZSelectItem> mapPrefixSelectItemsAux = SQLUtility.getSelectItemsMapPrefix(oldSelectItems);

		String subOpSQLAlias = opProjectSubOpSQL.generateAlias();

		Collection<ZSelectItem> newSelectItemsVar = new LinkedList<ZSelectItem>();
		List<Var> selectVars = opProject.getVars();
		Collection<ZSelectItem> newSelectItemsMappingId = new Vector<ZSelectItem>();
		for(Var selectVar : selectVars) {
//			Collection<ZSelectItem> selectItemsByVars = this.generateSelectItem(
//					selectVar, subOpSQLAlias, oldSelectItems, true);
			Collection<ZSelectItem> selectItemsByVars = selectItemGenerator.generateSelectItem(
					selectVar, subOpSQLAlias, oldSelectItems, true);
			newSelectItemsVar.addAll(selectItemsByVars);

			Collection<ZSelectItem> mapPrefixSelectItemsAux = MorphSQLUtility.getSelectItemsMapPrefix(
					oldSelectItems, selectVar, subOpSQLAlias, this.databaseType);
			newSelectItemsMappingId.addAll(mapPrefixSelectItemsAux);
		}

		Collection<ZSelectItem> newSelectItems = new Vector<ZSelectItem>(newSelectItemsVar);
		newSelectItems.addAll(newSelectItemsMappingId);
		
		if(this.optimizer != null && this.optimizer.isSubQueryAsView()) {
			Connection conn = this.getConnection();
			String subQueryViewName = "sqp" + Math.abs(opProject.hashCode());
			String dropViewSQL = "DROP VIEW IF EXISTS " + subQueryViewName;
			logger.info(dropViewSQL + ";\n");
			DBUtility.execute(conn, dropViewSQL);
			String createViewSQL = "CREATE VIEW " + subQueryViewName + " AS " + opProjectSubOpSQL;
			logger.info(createViewSQL  + ";\n");
			DBUtility.execute(conn, createViewSQL);
			new SQLFromItem(subQueryViewName, LogicalTableType.TABLE_NAME, this.databaseType);
		}

		IQuery transProjectSQL;
		if(this.optimizer != null && this.optimizer.isSubQueryElimination()) {
			
			//push group by down
			opProjectSubOpSQL.pushGroupByDown();
			
			//pushing down projections
			opProjectSubOpSQL.pushProjectionsDown(newSelectItems);

			//pushing down order by
			opProjectSubOpSQL.pushOrderByDown(newSelectItems);

			transProjectSQL = opProjectSubOpSQL;
//			transProjectSQL.addSelectItems(newSelectItemsMappingId);
		} else {
			SQLQuery resultAux = new SQLQuery(opProjectSubOpSQL);
			resultAux.setSelectItems(newSelectItems);
			Vector<ZOrderBy> orderByConditions = opProjectSubOpSQL.getOrderBy();
			if(orderByConditions != null) {
				resultAux.pushOrderByDown(newSelectItems);
				opProjectSubOpSQL.setOrderBy(null);				
			}
			transProjectSQL = resultAux;
//			transProjectSQL.addSelectItems(newSelectItemsMappingId);
		}

		
		return transProjectSQL;
	}

	protected IQuery trans(OpSlice opSlice) throws Exception {
		long sliceLength = opSlice.getLength();
		long offset = opSlice.getStart();

		Op opSliceSubOp = opSlice.getSubOp();
		IQuery sqlQuery = this.trans(opSliceSubOp);
		if(sliceLength > 0) {
			sqlQuery.setSlice(sliceLength);	
		}

		if(offset > 0) {
			sqlQuery.setOffset(offset);	
		}

		return sqlQuery;
	}

	protected IQuery trans(OpUnion opUnion) throws Exception {
		Op gp1 = opUnion.getLeft();
		Op gp2 = opUnion.getRight();
		IQuery r1 = this.trans(gp1);
		IQuery r2 = this.trans(gp2);

		IQuery transUnion;
		if(r1 == null && r2 == null) {
			transUnion = null;
		} else if(r1 == null && r2 != null) {
			transUnion = r2;
		} else if(r1 != null && r2 == null) {
			transUnion = r1;
		} else {
			MorphSQLSelectItemGenerator selectItemGenerator = 
					new MorphSQLSelectItemGenerator(this.nameGenerator, this.databaseType);
			
			IQuery r3 = this.trans(gp2);
			IQuery r4 = this.trans(gp1);

			String r1Alias = r1.generateAlias() + "r1";
			r1.setAlias(r1Alias);
			String r2Alias = r2.generateAlias() + "r2";
			r2.setAlias(r2Alias);
			String r3Alias = r3.generateAlias() + "r3";
			r3.setAlias(r3Alias);
			String r4Alias = r4.generateAlias() + "r4";
			r4.setAlias(r4Alias);

			Collection<ZSelectItem> r1SelectItems = r1.getSelectItems();
			Collection<ZSelectItem> r2SelectItems = r2.getSelectItems();

			Collection<Node> termsGP1 = QueryTranslatorUtility.terms(gp1, this.ignoreRDFTypeStatement);
			Collection<Node> termsGP2 = QueryTranslatorUtility.terms(gp2, this.ignoreRDFTypeStatement);
			Set<Node> termsA = new LinkedHashSet<Node>(termsGP1);termsA.removeAll(termsGP2);
			Set<Node> termsB = new LinkedHashSet<Node>(termsGP2);termsB.removeAll(termsGP1);
			Set<Node> termsC = new LinkedHashSet<Node>(termsGP1);termsC.retainAll(termsGP2);

			
			//Collection<ZSelectItem> selectItemsA1 = this.generateSelectItems(termsA, r1Alias, r1SelectItems, false);
			Collection<ZSelectItem> selectItemsA1 = selectItemGenerator.generateSelectItems(
					termsA, r1Alias, r1SelectItems, false);
			MorphSQLUtility.setDefaultAlias(selectItemsA1);
			Collection<ZSelectItem> selectItemsB1 = selectItemGenerator.generateSelectItems(
					termsB, r2Alias, r2SelectItems, false);
			MorphSQLUtility.setDefaultAlias(selectItemsB1);
			Collection<ZSelectItem> selectItemsC1 = selectItemGenerator.generateSelectItems(
					termsC, r1Alias, r1SelectItems, false);
			MorphSQLUtility.setDefaultAlias(selectItemsC1);
			
			Collection<ZSelectItem> selectItemsMappingId1 = new Vector<ZSelectItem>();
			Collection<ZSelectItem> a1MappingIdSelectItems = this.generateMappingIdSelectItems(
					termsA, r1SelectItems, r1Alias);
			selectItemsMappingId1.addAll(a1MappingIdSelectItems);
			Collection<ZSelectItem> b1MappingIdSelectItems = this.generateMappingIdSelectItems(
					termsB, r2SelectItems, r2Alias);
			selectItemsMappingId1.addAll(b1MappingIdSelectItems);
			Collection<ZSelectItem> c1MappingIdSelectItems = this.generateMappingIdSelectItems(
					termsC, r1SelectItems, r1Alias);
			selectItemsMappingId1.addAll(c1MappingIdSelectItems);

			SQLQuery query1 = new SQLQuery();
			query1.setDatabaseType(this.databaseType);
			SQLJoinTable r1JoinTable = new SQLJoinTable(r1, null, null);
			SQLJoinTable r2JoinTable = new SQLJoinTable(
					r2, Constants.JOINS_TYPE_LEFT(), Constants.SQL_EXPRESSION_FALSE());
			query1.addFromItem(r1JoinTable);
			query1.addFromItem(r2JoinTable);
			query1.addSelectItems(selectItemsA1);
			query1.addSelectItems(selectItemsB1);
			query1.addSelectItems(selectItemsC1);
			query1.addSelectItems(selectItemsMappingId1);
			

			Collection<ZSelectItem> r3SelectItems = r3.getSelectItems();
			Collection<ZSelectItem> selectItemsA2 = selectItemGenerator.generateSelectItems(
					termsA, r4Alias, r1SelectItems, false);
			MorphSQLUtility.setDefaultAlias(selectItemsA2);
			Collection<ZSelectItem> selectItemsB2 = selectItemGenerator.generateSelectItems(
					termsB, r3Alias, r2SelectItems, false);
			MorphSQLUtility.setDefaultAlias(selectItemsB2);
			Vector<Node> termsCList = new Vector<Node>(termsC);
			Collection<ZSelectItem> selectItemsC2 = selectItemGenerator.generateSelectItems(
					termsCList, r3Alias + ".", r3SelectItems, false);
			MorphSQLUtility.setDefaultAlias(selectItemsC2);

			Collection<ZSelectItem> selectItemsMappingId2 = new Vector<ZSelectItem>();
			Collection<ZSelectItem> a2MappingIdSelectItems = this.generateMappingIdSelectItems(
					termsA, r1SelectItems, r4Alias);
			selectItemsMappingId2.addAll(a2MappingIdSelectItems);
			Collection<ZSelectItem> b2MappingIdSelectItems = this.generateMappingIdSelectItems(
					termsB, r2SelectItems, r3Alias);
			selectItemsMappingId2.addAll(b2MappingIdSelectItems);
			Collection<ZSelectItem> c2MappingIdSelectItems = this.generateMappingIdSelectItems(
					termsC, r1SelectItems, r3Alias);
			selectItemsMappingId2.addAll(c2MappingIdSelectItems);

			SQLQuery query2 = new SQLQuery();
			query2.setDatabaseType(this.databaseType);
			SQLJoinTable r3JoinTable = new SQLJoinTable(r3, null, null);
			SQLJoinTable r4JoinTable = new SQLJoinTable(r4
					, Constants.JOINS_TYPE_LEFT(), Constants.SQL_EXPRESSION_FALSE());
			query2.addFromItem(r3JoinTable);
			query2.addFromItem(r4JoinTable);
			query2.addSelectItems(selectItemsA2);
			query2.addSelectItems(selectItemsB2);
			query2.addSelectItems(selectItemsC2);
			query2.addSelectItems(selectItemsMappingId2);


			SQLUnion transUnionSQL = new SQLUnion();
			transUnionSQL.setDatabaseType(this.databaseType);
			transUnionSQL.add(query1);
			transUnionSQL.add(query2);
			logger.debug("transUnionSQL = \n" + transUnionSQL);

			transUnion = transUnionSQL;
		}

		return transUnion;
	}

	protected IQuery trans(Triple tp) throws QueryTranslationException {
		IQuery result = null;

		Node tpSubject = tp.getSubject();
		Node tpPredicate = tp.getPredicate();
		Node tpObject = tp.getObject();
		if(tpPredicate.isURI() && RDF.type.getURI().equals(tpPredicate.getURI()) && tpObject.isURI()) {
			result = null;
		} else {
			Collection<AbstractConceptMapping> cms = this.mapInferredTypes.get(tpSubject);
			if(cms == null || cms.isEmpty()) {
				String errorMessage = "Undefined triplesMap for triple : " + tp;
				logger.warn(errorMessage);
				String errorMessage2 = "All class mappings will be used.";
				logger.warn(errorMessage2);
				cms = this.mappingDocument.getClassMappings();
				if(cms == null || cms.size() == 0) {
					String errorMessage3 = "Mapping document doesn't contain any class mappings!";
					throw new QueryTranslationException(errorMessage3);
				}				
			}

			List<IQuery> unionOfSQLQueries = new Vector<IQuery>();
			Iterator<AbstractConceptMapping> cmsIterator = cms.iterator();
			while(cmsIterator.hasNext()) {
				AbstractConceptMapping cm = cmsIterator.next();
				IQuery resultAux = this.trans(tp, cm);
				if(resultAux != null) {
					unionOfSQLQueries.add(resultAux);	
				}
			}

			if(unionOfSQLQueries.size() == 0) {
				result = null;
			} else if(unionOfSQLQueries.size() == 1) {
				result = unionOfSQLQueries.get(0);
			} else if(unionOfSQLQueries.size() > 1) {
				result = new SQLUnion(unionOfSQLQueries);
			}
		}

		return result;
	}

	protected IQuery trans(Triple tp, AbstractConceptMapping cm) 
			throws QueryTranslationException {
		IQuery result = null;

		Node tpPredicate = tp.getPredicate();
		if(tpPredicate.isURI()) {
			String predicateURI = tpPredicate.getURI();
			try {
				result = this.trans(tp, cm, predicateURI, false);	
			} catch(InsatisfiableSQLExpression e) {

			}
		} else if(tpPredicate.isVariable()) {
			String mappingDocumentURL = this.getMappingDocumentURL();
			Resource triplesMapResource = cm.getResource();
			Map<String, ColumnMetaData> columnsMetaData = cm.getLogicalTable().getColumnsMetaData();
			
			TriplePatternPredicateBounder tpBounder = new TriplePatternPredicateBounder(mappingDocumentURL, columnsMetaData);
//			tpBounder.setMapColumnsMetaData(columnsMetaData);
			
			Map<Resource, List<String>> boundedTriplePatterns = 
					tpBounder.expandUnboundedPredicateTriplePattern(tp, triplesMapResource);
			logger.debug("boundedTriplePatterns = " + boundedTriplePatterns);
			
			//List<String> validationResult = validator.expandUnboundedTriplePattern(tp, tm);
			Collection<AbstractPropertyMapping> pms = cm.getPropertyMappings();
			if(boundedTriplePatterns.size() != pms.size()) {
				logger.debug("boundedTriplePatterns.size() != pms.size()!");	
			}
			
			
			if(pms != null && pms.size() > 0) {
				List<IQuery> sqlQueries = new Vector<IQuery>();
				for(AbstractPropertyMapping pm : pms) {
					Resource propertyMappingResource = pm.getResource();
					List<String> boundedTriplePatternErrorMessages = 
							boundedTriplePatterns.get(propertyMappingResource);
					String predicateURI = pm.getMappedPredicateName();
					if(boundedTriplePatternErrorMessages == null || boundedTriplePatternErrorMessages.isEmpty()) {
						try {
							IQuery sqlQuery = this.trans(tp, cm, predicateURI, true);
							sqlQueries.add(sqlQuery);							
						} catch(InsatisfiableSQLExpression e) {
							logger.warn("-- Insatifiable sql while translating : " + predicateURI + " in " + cm.getConceptName());
							logger.warn(e.getMessage());
							logger.warn(boundedTriplePatternErrorMessages);
						}
					} else {
						logger.warn("-- Insatifiable sql while translating : " + predicateURI + " in " + cm.getConceptName());
						logger.warn(boundedTriplePatternErrorMessages);
					}
					
//					try {
//						if(boundedTriplePatternErrorMessages != null && !boundedTriplePatternErrorMessages.isEmpty()) {
//							logger.warn("Check TriplePatternPredicateBounder class!");
//						}
//						IQuery sqlQuery = this.trans(tp, cm, predicateURI, true);
//						sqlQueries.add(sqlQuery);
//					} catch(InsatisfiableSQLExpression e) {
//						if(boundedTriplePatternErrorMessages == null || boundedTriplePatternErrorMessages.isEmpty()) {
//							logger.warn("Check TriplePatternPredicateBounder class!");
//						} else if(boundedTriplePatternErrorMessages.size() > 1) {
//							logger.warn("Check TriplePatternPredicateBounder class!");
//						}
//						
//						logger.warn(e.getMessage());
//						logger.warn(boundedTriplePatternErrorMessages.get(0));
//					}
					
				}
				if(sqlQueries.size() == 1) {
					result = sqlQueries.iterator().next();
				} else if(sqlQueries.size() > 1) {
					result = new SQLUnion(sqlQueries);
				}
			}
		} else {
			throw new QueryTranslationException("invalid tp.predicate : " + tpPredicate);
		}

		return result;
	}

	protected abstract IQuery trans(Triple tp, AbstractConceptMapping cm
			, String predicateURI, AbstractPropertyMapping pm) 
					throws QueryTranslationException, InsatisfiableSQLExpression;

	protected IQuery trans(Triple tp, AbstractConceptMapping cm
			, String predicateURI, boolean unboundedPredicate) 
					throws QueryTranslationException, InsatisfiableSQLExpression {
		IQuery transTP = null;
		Collection<AbstractPropertyMapping> pms = 
				cm.getPropertyMappings(predicateURI);
		
		if(pms == null || pms.size() == 0 && !RDF.type.getURI().equalsIgnoreCase(predicateURI)) {
			String errorMessage = "Undefined mappings of predicate : " + predicateURI;
			throw new QueryTranslationException(errorMessage);			
		} else {
			
		}
		
		//alpha
		AlphaResult alphaResult = this.getAlphaGenerator().calculateAlpha(tp, cm, predicateURI);
		if(alphaResult != null) {
			SQLLogicalTable alphaSubject = alphaResult.getAlphaSubject();
			Collection<SQLJoinTable> alphaPredicateObjects = alphaResult.getAlphaPredicateObjects();

			//beta
			AbstractBetaGenerator betaGenerator = this.getBetaGenerator();

			//PRSQL
			AbstractPRSQLGenerator prSQLGenerator = this.getPrSQLGenerator();
			NameGenerator nameGenerator = this.getNameGenerator(); 
			Collection<ZSelectItem> prSQL = prSQLGenerator.genPRSQL(
					tp, alphaResult, betaGenerator, nameGenerator
					, cm, predicateURI, unboundedPredicate);

			//CondSQL
			AbstractCondSQLGenerator condSQLGenerator = 
					this.getCondSQLGenerator();
			CondSQLResult condSQLResult = condSQLGenerator.genCondSQL(tp, alphaResult, betaGenerator, cm, predicateURI);
			ZExpression condSQL = null;
			if(condSQLResult != null) {
				condSQL = condSQLResult.getExpression();
			}

			SQLQuery resultAux = null;
			//don't do subquery elimination here! why?
			if(this.optimizer != null && this.optimizer.isSubQueryElimination()) {
				try {
					//resultAux = this.createQuery(alphaSubject, alphaPredicateObjects, prSQL, condSQL);
					resultAux = SQLQuery.createQuery(alphaSubject, alphaPredicateObjects, prSQL, condSQL, this.databaseType);
				} catch(Exception e) {
					String errorMessage = "error in eliminating subquery!";
					logger.error(errorMessage);
					resultAux = null;
				}
			} 

			if(resultAux == null) { //without subquery elimination or error occured during the process
				resultAux = new SQLQuery(alphaSubject);
				if(alphaPredicateObjects != null) {
					for(SQLJoinTable alphaPredicateObject : alphaPredicateObjects) {
						if(alphaSubject instanceof SQLFromItem) {
							resultAux.addFromItem(alphaPredicateObject);//alpha predicate object	
						} else if(alphaSubject instanceof SQLQuery) {
							ZExpression onExpression = alphaPredicateObject.getOnExpression();
							alphaPredicateObject.setOnExpression(null);
							resultAux.addFromItem(alphaPredicateObject);//alpha predicate object
							resultAux.pushFilterDown(onExpression);
						} else {
							resultAux.addFromItem(alphaPredicateObject);//alpha predicate object	
						}
					}					
				}
				resultAux.setSelectItems(prSQL);
				resultAux.setWhere(condSQL);
			}

			transTP = resultAux;
			logger.debug("transTP(tp, cm) = " + transTP);			
		}

		return transTP;		
	}

	protected List<ZExp> transConstant(NodeValue nodeValue) {
		List<ZExp> result = new LinkedList<ZExp>();

		boolean isLiteral = nodeValue.isLiteral();
		//boolean isIRI = nodeValue.isIRI();
		boolean isIRI = nodeValue.getNode().isURI();

		if(isLiteral) {
			ZExp resultAux = this.transLiteral(nodeValue);
			result.add(resultAux);
		} else if(isIRI) {
			result = this.transIRI(nodeValue.getNode());
		}
		return result;
	}

	protected List<ZExp> transExpr(Op op, Expr expr
			, Collection<ZSelectItem> subOpSelectItems, String prefix) {
		List<ZExp> result = new LinkedList<ZExp>();

		if(expr.isVariable()) {
			logger.debug("expr is var");
			Var var = expr.asVar();
			List<ZExp> resultAuxs = this.transVar(var, subOpSelectItems, prefix);
			result.addAll(resultAuxs);
		} else if(expr.isConstant()) {
			logger.debug("expr is constant");
			NodeValue nodeValue = expr.getConstant();
			result = this.transConstant(nodeValue);
		} else if(expr.isFunction()) {
			logger.debug("expr is function");
			ExprFunction exprFunction = expr.getFunction();
			ZExp resultsAux = this.transFunction(op, exprFunction, subOpSelectItems, prefix);
			result.add(resultsAux); 
		}

		return result;
	}

	private ZExpression transExprList(Op op, ExprList exprList
			, Collection<ZSelectItem> subOpSelectItems, String prefix) {
		Collection<ZExp> resultAux = new Vector<ZExp>();
		List<Expr> exprs = exprList.getList();
		for(Expr expr : exprs) {
			List<ZExp> exprTranslated = this.transExpr(op, expr, subOpSelectItems, prefix);
			resultAux.addAll(exprTranslated);
		}
		ZExpression result = MorphSQLUtility.combineExpresions(resultAux, Constants.SQL_LOGICAL_OPERATOR_AND());
		return result;
	}

	private ZExp transFunction(Op op, ExprFunction exprFunction
			, Collection<ZSelectItem> subOpSelectItems, String prefix) {
		ZExp result;
		String functionSymbol = null;
		List<Expr> args = exprFunction.getArgs();

		if(exprFunction instanceof ExprFunction1) {
			Expr arg = args.get(0);

			if(exprFunction instanceof E_Bound) {
				//functionSymbol = "IS NOT NULL";
				functionSymbol = functionsMap.get(E_Bound.class.toString());
			} else if(exprFunction instanceof E_LogicalNot) {
				//functionSymbol = "NOT";
				functionSymbol = functionsMap.get(E_LogicalNot.class.toString());
			} else {
				functionSymbol = exprFunction.getOpName();
			}

			List<ZExp> argTranslated = this.transExpr(op, arg, subOpSelectItems, prefix);
			Collection<ZExp> resultAuxs = new Vector<ZExp>();
			for(int i=0; i<argTranslated.size(); i++ ) {
				ZExpression resultAux = new ZExpression(functionSymbol);
				ZExp operand = argTranslated.get(i);
				resultAux.addOperand(operand);
				resultAuxs.add(resultAux);
			}

			result = MorphSQLUtility.combineExpresions(resultAuxs, Constants.SQL_LOGICAL_OPERATOR_AND());
		} else if(exprFunction instanceof ExprFunction2) {
			Expr leftArg = args.get(0);
			Expr rightArg = args.get(1);
			List<ZExp> leftExprTranslated = this.transExpr(op, leftArg, subOpSelectItems, prefix);
			List<ZExp> rightExprTranslated = this.transExpr(op, rightArg, subOpSelectItems, prefix);

			if(exprFunction instanceof E_NotEquals){
				if(Constants.DATABASE_MONETDB().equalsIgnoreCase(databaseType)) {
					functionSymbol = "<>";
				} else {
					functionSymbol = "!=";
				}

				String concatSymbol;
				if(Constants.DATABASE_POSTGRESQL().equalsIgnoreCase(databaseType)) {
					concatSymbol = "||";
				} else {
					concatSymbol = "CONCAT";
				}

				ZExpression resultAux = new ZExpression(functionSymbol);

				//concat only when it has multiple arguments
				int leftExprTranslatedSize = leftExprTranslated.size();
				if(leftExprTranslatedSize == 1) {
					resultAux.addOperand(leftExprTranslated.get(0));
				} else if (leftExprTranslatedSize > 1) {
					ZExpression leftConcatOperand = new ZExpression(concatSymbol);
					for(int i=0; i<leftExprTranslated.size(); i++ ) {
						ZExp leftOperand = leftExprTranslated.get(i);
						if(Constants.DATABASE_POSTGRESQL().equalsIgnoreCase(databaseType) && leftOperand instanceof ZConstant) {
							String leftOperandValue = ((ZConstant) leftOperand).getValue();
							MorphSQLConstant leftOperandNew = MorphSQLConstant.apply(leftOperandValue
									, ((ZConstant) leftOperand).getType(), databaseType, "text");
							leftConcatOperand.addOperand(leftOperandNew);
						} else {
							leftConcatOperand.addOperand(leftOperand);	
						}
					}
					resultAux.addOperand(leftConcatOperand);					
				}


				int rightExprTranslatedSize = rightExprTranslated.size();
				if(rightExprTranslatedSize == 1) {
					resultAux.addOperand(rightExprTranslated.get(0));
				} else if (rightExprTranslatedSize > 1) {
					ZExpression rightConcatOperand = new ZExpression(concatSymbol);
					for(int i=0; i<rightExprTranslated.size(); i++ ) {
						ZExp rightOperand = rightExprTranslated.get(i);
						if(Constants.DATABASE_POSTGRESQL().equalsIgnoreCase(databaseType) && rightOperand instanceof ZConstant) {
							String rightOperandValue = ((ZConstant) rightOperand).getValue();
							MorphSQLConstant rightOperandNew = MorphSQLConstant.apply(rightOperandValue
									, ((ZConstant) rightOperand).getType(), databaseType, "text");
							rightConcatOperand.addOperand(rightOperandNew);
						} else {
							rightConcatOperand.addOperand(rightOperand);	
						}
					}
					resultAux.addOperand(rightConcatOperand);					
				}

				result = resultAux;
			}  else {
				if(exprFunction instanceof E_LogicalAnd) {
					//functionSymbol = "AND";
					functionSymbol = functionsMap.get(E_LogicalAnd.class.toString());
				} else if(exprFunction instanceof E_LogicalOr) {
					//functionSymbol = "OR";
					functionSymbol = functionsMap.get(E_LogicalOr.class.toString());
				} else {
					functionSymbol = exprFunction.getOpName();
				}

				Collection<ZExp> resultAuxs = new Vector<ZExp>();
				for(int i=0; i<leftExprTranslated.size(); i++ ) {
					ZExpression resultAux = new ZExpression(functionSymbol);
					ZExp leftOperand = leftExprTranslated.get(i);
					resultAux.addOperand(leftOperand);
					ZExp rightOperand = rightExprTranslated.get(i);
					resultAux.addOperand(rightOperand);
					resultAuxs.add(resultAux);
				}
				result = MorphSQLUtility.combineExpresions(resultAuxs, Constants.SQL_LOGICAL_OPERATOR_AND());
			}
			
		} else if(exprFunction instanceof E_Function) {
			List<ZExp> resultAuxs;
			E_Function eFunction = (E_Function) exprFunction;
			String functionIRI = eFunction.getFunctionIRI();
			List<Expr> exprs = eFunction.getArgs();
			if(exprs != null && exprs.size() == 1) {
				Expr expr= exprs.get(0);
				resultAuxs = this.transExpr(op, expr, subOpSelectItems, prefix);
				String resultAux = resultAuxs.get(0).toString();
				
				if(functionIRI.equals(XSDDatatype.XSDinteger.getURI())) {
					result = new ZConstant(resultAux, ZConstant.NUMBER);
				} else if(functionIRI.equals(XSDDatatype.XSDdouble.getURI())) {
					result = new ZConstant(resultAux, ZConstant.NUMBER);
				} else if(functionIRI.equals(XSDDatatype.XSDdate.getURI())) {
					result = new ZConstant(resultAux, ZConstant.UNKNOWN);
				} else if(functionIRI.equals(XSDDatatype.XSDtime.getURI())) {
					result = new ZConstant(resultAux, ZConstant.UNKNOWN);
				} else if(functionIRI.equals(XSDDatatype.XSDdateTime.getURI())) {
					result = new ZConstant(resultAux, ZConstant.UNKNOWN);
				} else {
					result = new ZConstant(resultAux, ZConstant.STRING);
				}
			} else {
				String errorMessage = "unimplemented function";
				logger.error(errorMessage);
				result = null;
			}
		} else {
			List<List<ZExp>> transArgs = new Vector<List<ZExp>>();
			for(int i=0; i<args.size(); i++) {
				Expr arg = args.get(i);
				List<ZExp> zExps = this.transExpr(op, arg, subOpSelectItems, prefix);
				List<ZExp> transArg = new Vector<ZExp>();

				for(ZExp zExp : zExps) {
					if(exprFunction instanceof E_Regex && i==1) {
						zExp = new ZConstant("%" + ((ZConstant)zExp).getValue() + "%", ZConstant.STRING);
					}
					transArg.add(zExp);
				}
				transArgs.add(transArg);
			}

			if(exprFunction instanceof E_Regex) {
				//functionSymbol = "LIKE";
				functionSymbol = functionsMap.get(E_Regex.class.toString());
			} else if(exprFunction instanceof E_OneOf) {
				//functionSymbol = "IN";
				functionSymbol = functionsMap.get(E_OneOf.class.toString());
			} else {
				functionSymbol = exprFunction.getOpName();
			}

			Collection<ZExp> resultAuxs = new Vector<ZExp>();
			int arg0Size = transArgs.get(0).size();
			for(int j=0; j<arg0Size; j++ ) {
				ZExpression resultAux = new ZExpression(functionSymbol);
				for(int i=0; i<args.size(); i++) {
					ZExp operand = transArgs.get(i).get(j);
					resultAux.addOperand(operand);
				}
				resultAuxs.add(resultAux);
			}
			result = MorphSQLUtility.combineExpresions(resultAuxs, Constants.SQL_LOGICAL_OPERATOR_AND());
		}

		return result;
	}

	protected abstract List<ZExp> transIRI(Node node);

	protected IQuery transJoin(Op opParent, Op gp1, Op gp2
			, String joinType) throws Exception  {
		logger.debug("entering transJoin");
		MorphSQLSelectItemGenerator selectItemGenerator = new MorphSQLSelectItemGenerator(this.nameGenerator, this.databaseType);
		if (opParent instanceof OpLeftJoin) {
			OpLeftJoin opLefJoin = (OpLeftJoin) opParent;
			ExprList opLeftJoinExpr = opLefJoin.getExprs();
			if(opLeftJoinExpr != null && opLeftJoinExpr.size() > 0) {
				gp2 = OpFilter.filterDirect(opLeftJoinExpr, gp2);
			}
		}

		IQuery transGP1SQL = this.trans(gp1);
		IQuery transGP2SQL = this.trans(gp2);

		if(transGP1SQL == null && transGP2SQL == null) {
			return null;
		} else if(transGP1SQL != null && transGP2SQL == null) {
			return transGP1SQL;
		} else if(transGP1SQL == null && transGP2SQL != null) {
			return transGP2SQL;
		} else {
			Collection<ZSelectItem> gp1SelectItems = transGP1SQL.getSelectItems();
			Collection<ZSelectItem> gp2SelectItems = transGP2SQL.getSelectItems();

			String transGP1Alias = transGP1SQL.generateAlias();
//			this.mapTransGP1Alias.put(opParent, transGP1Alias);
			SQLFromItem transGP1FromItem;
			if(this.optimizer != null && this.optimizer.isSubQueryAsView()) {
				Connection conn = this.getConnection();
				String subQueryViewName = "sql" + Math.abs(gp1.hashCode());
				String dropViewSQL = "DROP VIEW IF EXISTS " + subQueryViewName;
				logger.info(dropViewSQL + ";\n");
				DBUtility.execute(conn, dropViewSQL);
				String createViewSQL = "CREATE VIEW " + subQueryViewName + " AS " + transGP1SQL;
				logger.info(createViewSQL + ";\n");
				DBUtility.execute(conn, createViewSQL);
				transGP1FromItem = new SQLFromItem(subQueryViewName, LogicalTableType.TABLE_NAME, this.databaseType);
			} else {
				//SQLFromItem fromItem = new SQLFromItem(transGP.toString(), SQLFromItem.FORM_QUERY);
				transGP1FromItem = new SQLFromItem(transGP1SQL.toString(), LogicalTableType.QUERY_STRING, this.databaseType);
			}
			transGP1FromItem.setAlias(transGP1Alias);

			String transGP2Alias = transGP2SQL.generateAlias();
			SQLFromItem transGP2FromItem;
			if(this.optimizer != null && this.optimizer.isSubQueryAsView()) {
				Connection conn = this.getConnection();
				String subQueryViewName = "sqr" + Math.abs(gp2.hashCode());
				String dropViewSQL = "DROP VIEW IF EXISTS " + subQueryViewName;
				logger.info(dropViewSQL + ";\n");
				DBUtility.execute(conn, dropViewSQL);
				String createViewSQL = "CREATE VIEW " + subQueryViewName + " AS " + transGP2SQL;
				logger.info(createViewSQL + ";\n");
				DBUtility.execute(conn, createViewSQL);
				transGP2FromItem = new SQLFromItem(subQueryViewName, LogicalTableType.TABLE_NAME, this.databaseType);
			} else {
				transGP2FromItem = new SQLFromItem(transGP2SQL.toString(), LogicalTableType.QUERY_STRING, this.databaseType);
			}
			transGP2FromItem.setAlias(transGP2Alias);


			//			SQLJoinQuery joinQuery2 = new SQLJoinQuery();
			//			joinQuery2.setJoinType(joinType);
			//			joinQuery2.setJoinSource(transGP2FromItem);
			//joinQuery.addLogicalTable(transGP2FromItem);


			Collection<Node> termsGP1 = QueryTranslatorUtility.terms(gp1, this.ignoreRDFTypeStatement);
			Collection<Node> termsGP2 = QueryTranslatorUtility.terms(gp2, this.ignoreRDFTypeStatement);
			Set<Node> termsA = new HashSet<Node>(termsGP1);termsA.removeAll(termsGP2);
			Set<Node> termsB = new HashSet<Node>(termsGP2);termsB.removeAll(termsGP1);
			Set<Node> termsC = new HashSet<Node>(termsGP1);termsC.retainAll(termsGP2);
			Collection<ZSelectItem> mappingsSelectItems = new Vector<ZSelectItem>();
			for(Node termA : termsA) {
				if(termA.isVariable()) {
					Collection<ZSelectItem> mappingsSelectItemsAux = MorphSQLUtility.getSelectItemsMapPrefix(
							gp1SelectItems, termA, transGP1Alias, this.databaseType);
					mappingsSelectItems.addAll(mappingsSelectItemsAux);
				}
			}
			for(Node termB : termsB) {
				if(termB.isVariable()) {
					Collection<ZSelectItem> mappingsSelectItemsAux = MorphSQLUtility.getSelectItemsMapPrefix(
							gp2SelectItems, termB, transGP2Alias, this.databaseType);
					mappingsSelectItems.addAll(mappingsSelectItemsAux);
				}
			}
			for(Node termC : termsC) {
				if(termC.isVariable()) {
					Collection<ZSelectItem> mappingsSelectItemsAux = MorphSQLUtility.getSelectItemsMapPrefix(
							gp1SelectItems, termC, transGP2Alias, this.databaseType);
					mappingsSelectItems.addAll(mappingsSelectItemsAux);
				}
			}

//			this.mapTermsC.put(opParent, termsC);
			Collection<ZSelectItem> selectItems = new HashSet<ZSelectItem>();
//			Collection<ZSelectItem> selectItemsA = this.generateSelectItems(
//					termsA, transGP1Alias, gp1SelectItems, false);
			Collection<ZSelectItem> selectItemsA = selectItemGenerator.generateSelectItems(
					termsA, transGP1Alias, gp1SelectItems, false);
			
			for(ZSelectItem selectItemA : selectItemsA) {
				selectItemA.setAlias(selectItemA.getColumn());
			}
			selectItems.addAll(selectItemsA);
//			Collection<ZSelectItem> selectItemsB = this.generateSelectItems(
//					termsB, transGP2Alias, gp2SelectItems, false);
			Collection<ZSelectItem> selectItemsB = selectItemGenerator.generateSelectItems(
					termsB, transGP2Alias, gp2SelectItems, false);			
			for(ZSelectItem selectItemB : selectItemsB) {
				selectItemB.setAlias(selectItemB.getColumn());
			}			
			selectItems.addAll(selectItemsB);
//			Collection<ZSelectItem> selectItemsC = this.generateSelectItems(
//					termsC, transGP1Alias, gp1SelectItems, false);
			Collection<ZSelectItem> selectItemsC = selectItemGenerator.generateSelectItems(
					termsC, transGP1Alias, gp1SelectItems, false);			
			for(ZSelectItem selectItemC : selectItemsC) {
				selectItemC.setAlias(selectItemC.getColumn());
			}
			selectItems.addAll(selectItemsC);
			logger.debug("selectItems = " + selectItems);


			//.... JOIN ... ON <joinOnExpression>
			Collection<ZExpression> joinOnExps = new HashSet<ZExpression>();
			for(Node termC : termsC) {
				boolean isTermCInSubjectGP1 = SPARQLUtility.isNodeInSubjectGraph(termC, gp1);
				boolean isTermCInSubjectGP2 = SPARQLUtility.isNodeInSubjectGraph(termC, gp2);

				if(termC.isVariable()) {
					Collection<String> termCColumns1 = this.getColumnsByNode(termC, gp1SelectItems);
					Collection<String> termCColumns2 = this.getColumnsByNode(termC, gp2SelectItems);

					if(termCColumns1.size() == termCColumns2.size()) {
						Iterator<String> termCColumns1Iterator = termCColumns1.iterator();
						Iterator<String> termCColumns2Iterator = termCColumns2.iterator();

						Collection<ZExpression> exps1Aux = new Vector<ZExpression>();
						Collection<ZExpression> exps2Aux = new Vector<ZExpression>();
						Collection<ZExpression> exps3Aux = new Vector<ZExpression>();
						while(termCColumns1Iterator.hasNext()) {
							String termCColumn1 = termCColumns1Iterator.next();
							String termCColumn2 = termCColumns2Iterator.next();
							ZConstant gp1TermC = MorphSQLConstant.apply(transGP1Alias + "." + termCColumn1
									, ZConstant.COLUMNNAME, this.databaseType, null);
							ZConstant gp2TermC = MorphSQLConstant.apply(transGP2Alias + "." + termCColumn2
									, ZConstant.COLUMNNAME, this.databaseType, null);

							ZExpression exp1Aux = new ZExpression("=", gp1TermC, gp2TermC);
							exps1Aux.add(exp1Aux);

							if(!isTermCInSubjectGP1 && !(gp1 instanceof OpBGP)) {
								ZExpression exp2Aux = new ZExpression("IS NULL", gp1TermC);
								exps2Aux.add(exp2Aux);
							}

							if(!isTermCInSubjectGP2 && !(gp2 instanceof OpBGP)) {
								ZExpression exp3Aux = new ZExpression("IS NULL", gp2TermC);
								exps3Aux.add(exp3Aux);								
							}
						}
						ZExpression exp1 = MorphSQLUtility.combineExpresions(exps1Aux
								, Constants.SQL_LOGICAL_OPERATOR_AND());
						ZExpression exp2 = MorphSQLUtility.combineExpresions(exps2Aux
								, Constants.SQL_LOGICAL_OPERATOR_AND());
						ZExpression exp3 = MorphSQLUtility.combineExpresions(exps3Aux
								, Constants.SQL_LOGICAL_OPERATOR_AND());

						if(exps2Aux.isEmpty() && exps3Aux.isEmpty()) {
							if(exp1 != null) {
								joinOnExps.add(exp1);	
							}
						} else {
							ZExpression exp123 = new ZExpression(Constants.SQL_LOGICAL_OPERATOR_OR());
							if(exp1 != null) {
								exp123.addOperand(exp1);	
							}

							if(!isTermCInSubjectGP1 && exp2 != null) {
								exp123.addOperand(exp2);	
							}

							if(!isTermCInSubjectGP2 && exp3 != null) {
								exp123.addOperand(exp3);	
							}

							joinOnExps.add(exp123);							
						}
					}						
				}
			}

			if(joinOnExps == null || joinOnExps.size() == 0) {
				joinOnExps.add(Constants.SQL_EXPRESSION_TRUE());
			}
			ZExpression joinOnExpression = MorphSQLUtility.combineExpresions(joinOnExps
					, Constants.SQL_LOGICAL_OPERATOR_AND());

			IQuery transJoin = null;
			if(this.optimizer != null) {
				boolean isTransJoinSubQueryElimination = this.optimizer.isTransJoinSubQueryElimination();
				if(isTransJoinSubQueryElimination) {
					try {
						if(transGP1SQL instanceof SQLQuery && transGP2SQL instanceof SQLQuery) {
							//							Collection<SQLLogicalTable> logicalTables = new Vector<SQLLogicalTable>();
							//							logicalTables.add(transGP1SQL);
							//							logicalTables.add(transGP2SQL);
							transJoin = SQLQuery.create(selectItems, transGP1SQL, transGP2SQL, joinType, joinOnExpression, this.databaseType);
						}					
					} catch(Exception e) {
						String errorMessage = "error while eliminating subquery in transjoin: " + e.getMessage();
						logger.error(errorMessage);
						transJoin = null;
					}					
				}
			}

			if(transJoin == null) { //subquery not eliminated
				SQLJoinTable table1 = new SQLJoinTable(transGP1SQL, null, null);
				table1.setAlias(transGP1Alias);
				SQLJoinTable table2 = new SQLJoinTable(transGP2SQL, joinType, joinOnExpression);
				table2.setAlias(transGP2Alias);

				SQLQuery transJoinAux = new SQLQuery();
				transJoinAux.setSelectItems(selectItems);
				transJoinAux.addFromItem(table1);
				transJoinAux.addFromItem(table2);
				transJoin = transJoinAux;
			}

			transJoin.addSelectItems(mappingsSelectItems);
			return transJoin;
		}
	}

	public IQuery translate(Query sparqlQuery) throws Exception {
		final Op opSparqlQuery = Algebra.compile(sparqlQuery) ;
		logger.info("SPARQL query = \n" + opSparqlQuery);
		NodeTypeInferrer typeInferrer = new NodeTypeInferrer(
				this.mappingDocument);
		this.mapInferredTypes = typeInferrer.infer(sparqlQuery);
		logger.info("Inferred Types : \n" + typeInferrer.printInferredTypes());

		this.buildAlphaGenerator();
		this.alphaGenerator.setIgnoreRDFTypeStatement(this.ignoreRDFTypeStatement);
		boolean subQueryAsView = this.optimizer != null && this.optimizer.isSubQueryAsView();
		this.alphaGenerator.setSubqueryAsView(subQueryAsView);
		this.buildBetaGenerator();
		this.buildPRSQLGenerator();
		this.prSQLGenerator.setIgnoreRDFTypeStatement(this.ignoreRDFTypeStatement);
		this.buildCondSQLGenerator();
		this.condSQLGenerator.setIgnoreRDFTypeStatement(this.ignoreRDFTypeStatement);
		logger.debug("opSparqlQuery = " + opSparqlQuery);
		long start = System.currentTimeMillis();

		IQuery result = null;
		//logger.info("opSparqlQuery = " + opSparqlQuery);
		if(this.optimizer != null && this.optimizer.isSelfJoinElimination()) {
			Map<Node, Long> mapNodeLogicalTableSize = new HashMap<Node, Long>();
			for(Node node : mapInferredTypes.keySet()) {
				Set<AbstractConceptMapping> cms = mapInferredTypes.get(node);
				AbstractConceptMapping cm = cms.iterator().next();
				Long logicalTableSize = cm.getLogicalTableSize();
				mapNodeLogicalTableSize.put(node, logicalTableSize);
			}
			
			boolean reorderSTG = true;
			if(this.configurationProperties != null) {
				reorderSTG = this.configurationProperties.isReorderSTG();
			}
			MorphQueryRewriter queryRewritter = new MorphQueryRewriter(mapNodeLogicalTableSize, reorderSTG);
			
//			queryRewritter.setMapInferredTypes(mapInferredTypes);
			Op opSparqlQuery2;
			try {
				opSparqlQuery2 = queryRewritter.rewrite(opSparqlQuery);	
			} catch (Exception e) {
				e.printStackTrace();
				opSparqlQuery2 = opSparqlQuery;
			}
			
			logger.debug("opSparqlQueryRewritten = \n" + opSparqlQuery2);
			result = this.trans(opSparqlQuery2);
		} else {
			result = this.trans(opSparqlQuery);
		}

		//		if(!(opSparqlQuery instanceof OpProject)) {
		//			Collection<Var> allVars = OpVars.allVars(opQueryPattern);
		//			logger.info("vars in query pattern = " + allVars);
		//			List<Var> vars = new Vector<Var>(allVars);
		//			opSparqlQuery = new OpProject(opSparqlQuery, vars); 
		//		}

		if(result != null) {
			result.cleanupSelectItems();
			result.cleanupOrderBy();
		}

		long end = System.currentTimeMillis();
		logger.debug("Query translation time = "+ (end-start)+" ms.");

		logger.debug("sql = \n" + result + "\n");
		return result;
	}

	//	public Map<String, Object> getMapVarMapping2() {
	//		return this.mapVarMapping2;
	//	}

	public IQuery translateFromQueryFile(String queryFilePath) throws Exception {
		//process SPARQL file
		logger.info("Parsing query file : " + queryFilePath);
		Query sparqlQuery = QueryFactory.read(queryFilePath);
		logger.debug("sparqlQuery = " + sparqlQuery);

		return this.translate(sparqlQuery);
	}


	public IQuery translateFromString(String queryString) throws Exception {
		//process SPARQL string
		logger.debug("Parsing query string : " + queryString);
		Query sparqlQuery = QueryFactory.create(queryString);
		logger.debug("sparqlQuery = " + sparqlQuery);

		return this.translate(sparqlQuery);
	}


	public abstract String translateResultSet(String varName, AbstractResultSet rs);


	private ZExp transLiteral(NodeValue nodeValue) {
		ZExp result = null;
		
/*		logger.info(nodeValue.isBlank());
		logger.info(nodeValue.isBoolean());
		logger.info(nodeValue.isConstant());
		logger.info(nodeValue.isDate());
		logger.info(nodeValue.isDateTime());
		logger.info(nodeValue.isDecimal());
		logger.info(nodeValue.isDouble());
		logger.info(nodeValue.isDuration());
		logger.info(nodeValue.isExpr());
		logger.info(nodeValue.isFloat());
		logger.info(nodeValue.isFunction());
		logger.info(nodeValue.isInteger());
		logger.info(nodeValue.isIRI());
		logger.info(nodeValue.isLiteral());
		logger.info(nodeValue.isNumber());
*/		
		if(nodeValue.isNumber()) {
			double nodeValueDouble = nodeValue.getDouble();
			result = new ZConstant(nodeValueDouble + "", ZConstant.NUMBER);
		} else if(nodeValue.isString()) {
			String nodeValueString = nodeValue.getString();
			result = new ZConstant(nodeValueString + "", ZConstant.STRING);
		} else if(nodeValue.isDate() || nodeValue.isDateTime()) {
			String nodeValueDateTimeString = nodeValue.getDateTime().toString().replaceAll("T", " ");
			result = new ZConstant(nodeValueDateTimeString, ZConstant.STRING);
		} else if(nodeValue.isLiteral()) {
			Node node = nodeValue.getNode();
			String literalLexicalForm = node.getLiteralLexicalForm();
			String literalDatatypeURI = node.getLiteralDatatypeURI();
			if(XSD.date.toString().equals(literalDatatypeURI) || XSD.dateTime.equals(literalDatatypeURI)) {
				String  literalValueString = literalLexicalForm.replaceAll("T", " ");
				result = new ZConstant(literalValueString, ZConstant.STRING);
			} else {
				result = new ZConstant(literalLexicalForm.toString(), ZConstant.STRING);
			}
		} else {
			result = new ZConstant(nodeValue.toString(), ZConstant.STRING);
		}
		return result;
	}


	private IQuery transSTG(List<Triple> stg) throws Exception {
		IQuery result = null;

		Node stgSubject = stg.get(0).getSubject();
		Collection<AbstractConceptMapping> cms = this.mapInferredTypes.get(stgSubject);
		if(cms == null) {
			String errorMessage = "Undefined triplesMap for stg : " + stg;
			logger.warn(errorMessage);
			String errorMessage2 = "All class mappings will be used.";
			logger.warn(errorMessage2);
			cms = this.mappingDocument.getClassMappings();
			if(cms == null || cms.size() == 0) {
				String errorMessage3 = "Mapping document doesn't contain any class mappins!";
				throw new QueryTranslationException(errorMessage3);
			}				
		}

		Collection<IQuery> resultAux = new Vector<IQuery>();
		for(AbstractConceptMapping cm : cms) {
			IQuery sqlQuery = this.transSTG(stg, cm);
			resultAux.add(sqlQuery);
		}

		if(resultAux.size() == 1) {
			result = resultAux.iterator().next();
		} else if(cms.size() > 1) {
			result = new SQLUnion(resultAux);
		}

		return result;
	}


	private IQuery transSTG(List<Triple> stg
			, AbstractConceptMapping cm) throws Exception {
		IQuery transSTG;

		//AlphaSTG
		List<AlphaResultUnion> alphaResultUnionList = 
				this.alphaGenerator.calculateAlphaSTG(stg, cm);

		//check if no union in each of alpha tp
		boolean unionFree = true;
		for(AlphaResultUnion alphaTP : alphaResultUnionList) {
			if(alphaTP.size() > 1) {
				unionFree = false;
			}
		}

		if(!unionFree) {
			BasicPattern basicPatternHead = new BasicPattern();
			basicPatternHead.add(stg.get(0));
			OpBGP opBGPHead = new OpBGP(basicPatternHead);

			List<Triple> triplesTail = stg.subList(1, stg.size());
			BasicPattern basicPatternTail = BasicPattern.wrap(triplesTail);
			OpBGP opBGPTail = new OpBGP(basicPatternTail);

			Op opJoin = OpJoin.create(opBGPHead, opBGPTail);
			transSTG = this.trans(opJoin);
		} else {// no union in alpha
			//ALPHA(stg) returns the same result for subject
			AlphaResult alphaResult = alphaResultUnionList.get(0).get(0);
			SQLLogicalTable alphaSubject = alphaResult.getAlphaSubject();
			Collection<SQLJoinTable> alphaPredicateObjects = new Vector<SQLJoinTable>();
			for(AlphaResultUnion alphaTP : alphaResultUnionList) {
				Collection<SQLJoinTable> tpAlphaPredicateObjects = alphaTP.get(0).getAlphaPredicateObjects();
				alphaPredicateObjects.addAll(tpAlphaPredicateObjects);
			}

			//PRSQLSTG
			Collection<ZSelectItem> prSQLSTG = this.prSQLGenerator.genPRSQLSTG(stg, alphaResult, betaGenerator, nameGenerator, cm);


			//CondSQLSTG
			ZExpression condSQLSQL = this.condSQLGenerator.genCondSQLSTG(stg, alphaResult, betaGenerator, cm);

			//TRANS(STG)
			SQLQuery resultAux = null;
			//don't do subquery elimination here! why?
			if(this.optimizer != null) {
				boolean isTransSTGSubQueryElimination = this.optimizer.isTransSTGSubQueryElimination();
				if(isTransSTGSubQueryElimination) {
					try {
						//resultAux = this.createQuery(alphaSubject, alphaPredicateObjects, prSQLSTG, condSQLSQL);
						resultAux = SQLQuery.createQuery(alphaSubject, alphaPredicateObjects, prSQLSTG, condSQLSQL, this.databaseType);
					} catch(Exception e) {
						String errorMessage = "error in eliminating subquery!";
						logger.error(errorMessage);
						resultAux = null;
					}					
				}
			}


			if(resultAux == null) { //without subquery elimination or error occured during the process
				resultAux = new SQLQuery(alphaSubject);
				resultAux.setDatabaseType(this.databaseType);
				for(SQLJoinTable alphaPredicateObject : alphaPredicateObjects) {
					resultAux.addFromItem(alphaPredicateObject);//alpha predicate object
				}
				resultAux.setSelectItems(prSQLSTG);
				resultAux.setWhere(condSQLSQL);
			}

			transSTG = resultAux;
		}

		logger.debug("transSTG = " + transSTG);
		return transSTG;
	}

	private List<ZExp> transVar(Var var, Collection<ZSelectItem> subOpSelectItems, String prefix) {
		//		String nameVar = nameGenerator.generateName(var);
		//		ZExp zExp = new ZConstant(nameVar, ZConstant.COLUMNNAME);

		Collection<String> columns = this.getColumnsByNode(var, subOpSelectItems);
		List<ZExp> result = new LinkedList<ZExp>();
		for(String column : columns) {
			String columnName;
			if(prefix == null) {
				columnName = column;
			} else {
				if(!prefix.endsWith(".")) {
					prefix += ".";
				}
				columnName = prefix + column;
			}

			ZConstant constant = MorphSQLConstant.apply(columnName, ZConstant.COLUMNNAME
					, this.databaseType, null);
			result.add(constant);
		}
		return result;
	}

	public abstract String getTripleAlias(Triple tp);
	public abstract void putTripleAlias(Triple tp, String alias);

	@Override
	public Query getSPARQLQuery() {return this.sparqQuery;}
	
	@Override
	public void setSPARQLQuery(Query query) {this.sparqQuery = query;}
	
	@Override
	public void setSPARQLQueryByString(String sparqlQueryString) {
		Query sparqQuery = QueryFactory.create(sparqlQueryString);
		this.setSPARQLQuery(sparqQuery);
	}

	@Override
	public void setSPARQLQueryByFile(String queryFilePath) {
		if(queryFilePath != null && !queryFilePath.equals("") ) {
			logger.info("Parsing query file : " + queryFilePath);
			Query sparqQuery = QueryFactory.read(queryFilePath);
			this.setSPARQLQuery(sparqQuery);
		}
	}
	
//	private Collection<ZSelectItem> generateSelectItem(Node node, String prefix
//			, Collection<ZSelectItem> oldSelectItems, boolean useAlias) {
//		Collection<ZSelectItem> result = new LinkedList<ZSelectItem>();
//		if(prefix != null && !prefix.endsWith(".")) {
//			prefix += ".";
//		}
//
//		String nameSelectVar = nameGenerator.generateName(node);
//
//		ZSelectItem newSelectItem = null;
//		if(node.isVariable() || node.isURI()) {
//			if(oldSelectItems == null) {
//				if(prefix == null) {
//					newSelectItem = new ZSelectItem(nameSelectVar);
//				} else {
//					newSelectItem = new ZSelectItem(prefix + nameSelectVar);	
//				}
//
//				if(useAlias) {
//					newSelectItem.setAlias(nameSelectVar);	
//				}
//				result.add(newSelectItem);
//			} else {
//				Iterator<ZSelectItem> oldSelectItemsIterator = oldSelectItems.iterator();
//				while(oldSelectItemsIterator.hasNext()) {
//					ZSelectItem oldSelectItem = oldSelectItemsIterator.next(); 
//					String selectItemName;
//					String oldAlias = oldSelectItem.getAlias();
//					String oldTable = null;
//					if(oldAlias == null || oldAlias.equals("")) {
//						selectItemName = oldSelectItem.getColumn();
//						oldTable = oldSelectItem.getTable();
//					} else {
//						selectItemName = oldAlias; 
//					}
//
//					String newSelectItemAlias = null;
//					if(selectItemName.equalsIgnoreCase(nameSelectVar)) {
//						if(prefix == null || prefix.equals("")) {
//							if(oldTable == null || oldTable.equals("")) {
//								newSelectItem = new ZSelectItem(selectItemName);
//							} 
//														else {
//															newSelectItem = new ZSelectItem(oldTable + "." + selectItemName);
//														}
//						} else {
//							newSelectItem = new ZSelectItem(prefix + selectItemName);
//						}
//
//						if(node.isVariable()) {
//							newSelectItemAlias = node.getName();	
//						} else if(node.isURI()) {
//							newSelectItemAlias = node.getURI();
//						}
//					} else if (selectItemName.contains(nameSelectVar + "_")) {
//						if(prefix == null || prefix.equals("")) {
//							if(oldTable == null || oldTable.equals("")) {
//								newSelectItem = new ZSelectItem(selectItemName);
//							} else {
//								newSelectItem = new ZSelectItem(oldTable + "." + selectItemName);
//							}
//						} else {
//							newSelectItem = new ZSelectItem(prefix + selectItemName);
//						}
//
//						if(node.isVariable()) {
//							newSelectItemAlias = node.getName();	
//						} else if(node.isURI()) {
//							newSelectItemAlias = node.getURI();
//						}						
//						newSelectItemAlias += selectItemName.replaceAll(nameSelectVar, "");
//					} else {
//						newSelectItem = null;
//					}
//
//					if(newSelectItem != null) {
//						if(newSelectItemAlias != null && useAlias) {
//							newSelectItem.setAlias(newSelectItemAlias);
//						}
//						result.add(newSelectItem);
//					}
//				}	
//			}
//		} else if(node.isLiteral()){
//			Object literalValue = node.getLiteralValue();
//			ZExp exp;
//			String constantValue;
//			if(prefix == null) {
//				constantValue = literalValue.toString();
//			} else {
//				constantValue = prefix + literalValue.toString();
//			}
//
//			if(literalValue instanceof String) {
//				exp = new ZConstant(constantValue, ZConstant.STRING);							
//			} else if (literalValue instanceof Double) {
//				exp = new ZConstant(constantValue, ZConstant.NUMBER);
//			} else {
//				exp = new ZConstant(constantValue, ZConstant.STRING);							
//
//			}
//			newSelectItem = new SQLSelectItem();
//			newSelectItem.setExpression(exp);
//			if(useAlias) { newSelectItem.setAlias(nameSelectVar); }
//
//			result.add(newSelectItem);
//		} else {
//			logger.warn("unsupported node " + node.toString());
//		}
//
//		return result;
//	}
//
//
//	private Collection<ZSelectItem> generateSelectItems(Collection<Node> nodes
//			, String prefix, Collection<ZSelectItem> oldSelectItems, boolean useAlias) {
//		if(prefix != null && !prefix.endsWith(".")) {
//			prefix += ".";
//		}		
//
//		Collection<ZSelectItem> result = new LinkedHashSet<ZSelectItem>();
//
//		for(Node node : nodes) {
//			if(!node.isLiteral()) {
//				Collection<ZSelectItem> selectItems = this.generateSelectItem(
//						node, prefix, oldSelectItems, useAlias);
//				result.addAll(selectItems);				
//			}
//
//		}
//
//		return result;
//	}	

}
