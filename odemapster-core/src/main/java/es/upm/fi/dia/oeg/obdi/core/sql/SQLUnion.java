package es.upm.fi.dia.oeg.obdi.core.sql;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.Vector;

import Zql.ZExp;
import Zql.ZGroupBy;
import Zql.ZOrderBy;
import Zql.ZSelectItem;
import es.upm.fi.dia.oeg.morph.base.CollectionUtility;
import es.upm.fi.dia.oeg.morph.base.Constants;
import es.upm.fi.dia.oeg.morph.base.MorphSQLUtility;
import es.upm.fi.dia.oeg.upm.morph.sql.MorphSQLSelectItem;

public class SQLUnion implements IQuery {
	String alias;
	private String databaseType;
	private Collection<SQLQuery> unionQueries = new Vector<SQLQuery>();
	private Vector<ZOrderBy> orderByConditions;
	private String joinType;
	private ZExp onExp;
	private long slice = -1;
	private long offset = -1;
	private ZGroupBy groupBy;
	
	public SQLUnion() { 
		this.unionQueries = new Vector<SQLQuery>();
	}

	public SQLUnion(Collection<? extends IQuery> queries) {
		this.unionQueries = new Vector<SQLQuery>();
		
		for(IQuery query : queries) {
			this.add(query);
		}
	}

	public void add(IQuery newQuery) {
		if(this.unionQueries == null) {
			this.unionQueries = new Vector<SQLQuery>();
		}
		
		if(newQuery != null) {
			if(newQuery instanceof SQLQuery) {
				this.unionQueries.add((SQLQuery) newQuery);	
			} else if(newQuery instanceof SQLUnion) {
				Collection<SQLQuery> queries = ((SQLUnion) newQuery).getUnionQueries();
				for(SQLQuery sqlQuery : queries) {
					this.unionQueries.add(sqlQuery);
				}
			}
		}
	}

	public Collection<SQLQuery> getUnionQueries() {
		return unionQueries;
	}

	@Override
	public String toString() {
		String result = null;
		String unionString = "\n" + Constants.SQL_KEYWORD_UNION() + "\n" ;
		
		if(this.unionQueries != null) {
			Collection<Object> unionQueriesCollection = new Vector<Object>();
			StringBuffer stringBuffer = new StringBuffer();
			for(IQuery sqlQuery : this.unionQueries) {
				unionQueriesCollection.add(sqlQuery);
				String sqlQueryString = sqlQuery.toString(); 
				stringBuffer.append(sqlQueryString);
				stringBuffer.append(unionString);
			}
			result = CollectionUtility.mkString(unionQueriesCollection, 
					"\n" + Constants.SQL_KEYWORD_UNION() + "\n", "", "\n");
		}
		
		if(this.orderByConditions != null && this.orderByConditions.size() > 0) {
			Collection<Object> orderByConditionsCollection = new Vector<Object>();
			for(ZOrderBy orderBy : this.orderByConditions) {
				orderByConditionsCollection.add(orderBy);
			}
			
			String orderByString = CollectionUtility.mkString(orderByConditionsCollection, 
					", ", Constants.SQL_KEYWORD_ORDER_BY() + " ", " ");
			result = result + orderByString;
		}
			
		if(this.slice > 0) {
			result = result + "\n" + "LIMIT " + this.slice; 
		}
		
		if(this.offset> 0) {
			result = result + "\n" + "OFFSET " + this.offset; 
		}

		
		return result;
	}

	public String generateAlias() {
		if(this.alias == null) {
			this.alias = Constants.VIEW_ALIAS() + new Random().nextInt(10000);
		}
		return this.alias;
	}

	public LinkedList<String> getSelectItemAliases() {
		return this.unionQueries.iterator().next().getSelectItemAliases();
	}

	public void cleanupSelectItems() {
		if(this.unionQueries != null) {
			for(SQLQuery sqlQuery : this.unionQueries) {
				sqlQuery.cleanupSelectItems();
			}
		}
		
	}

	public void setOrderBy(Vector<ZOrderBy> orderByConditions) {
		this.orderByConditions = orderByConditions;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	public String getAlias() {
		return this.alias;
	}

	public Vector<ZOrderBy> getOrderBy() {
		return this.orderByConditions;
	}

//	public IQuery removeSubQuery(Collection<ZSelectItem> newSelectItems,
//			ZExpression newWhereCondition, Vector<ZOrderBy> orderByConditions,
//			String databaseType) throws Exception {
//		SQLUnion result = new SQLUnion();
//		Iterator<SQLQuery> it = this.unionQueries.iterator();
//		while(it.hasNext()) {
//			IQuery sqlQuery = it.next();
//			IQuery resultAux;
//			if(it.hasNext()) {
//				resultAux = sqlQuery.removeSubQuery(
//						newSelectItems, newWhereCondition, null, databaseType);				
//			} else {
//				resultAux = sqlQuery.removeSubQuery(
//						newSelectItems, newWhereCondition, orderByConditions, databaseType);				
//			}
//			result.add(resultAux);
//		}
//		return result;
//	}

	public Collection<ZSelectItem> getSelectItems() {
		Collection<ZSelectItem> result = new Vector<ZSelectItem>();
		
		Collection<ZSelectItem> firstQuerySelectItems = this.unionQueries.iterator().next().getSelectItems();
		for(ZSelectItem selectItem : firstQuerySelectItems) {
			String selectItemAlias = selectItem.getAlias();
			String newColumnName;
			String columnType;
			
			if(selectItemAlias == null || selectItemAlias.equals("")) {
				if(selectItem.isExpression()) {
					newColumnName = selectItem.getExpression().toString();
				} else {
					newColumnName = selectItem.getColumn();	
				}
			} else {
				newColumnName = selectItemAlias;
			}
			
			if(selectItem instanceof MorphSQLSelectItem) {
				MorphSQLSelectItem morphSQLSelectItem = (MorphSQLSelectItem) selectItem;
				columnType = morphSQLSelectItem.columnType();
			} else {
				columnType = null;
			}
			
			ZSelectItem newSelectItem = MorphSQLSelectItem.apply(
					newColumnName, null, this.getDatabaseType(), columnType);
			result.add(newSelectItem);
		}
		
		return result;
	}

	public void setSelectItems(Collection<ZSelectItem> newSelectItems) {
		for(SQLQuery query : this.unionQueries) {
			query.setSelectItems(newSelectItems);
		}		
	}

	public void addWhere(ZExp newWhere) {
		for(SQLQuery query : this.unionQueries) {
			query.addWhere(newWhere);
		}		
	}

	public void cleanupOrderBy() {
		for(SQLQuery query : this.unionQueries) {
			query.cleanupOrderBy();
		}
	}

//	public IQuery removeSubQuery() throws Exception {
//		SQLUnion result = new SQLUnion();
//		Iterator<SQLQuery> it = this.unionQueries.iterator();
//		while(it.hasNext()) {
//			SQLQuery query = it.next();
//			IQuery resultAux = query.removeSubQuery();
////			if(!it.hasNext()) {
////				resultAux.setOrderBy(this.orderByConditions);
////			}
//			result.add(resultAux);
//		}
//
//		result.setOrderBy(this.orderByConditions);
//		return result;
//	}

	public String getDatabaseType() {
		return databaseType;
	}

	public void setDatabaseType(String databaseType) {
		this.databaseType = databaseType;
	}

	public void setJoinType(String joinType) {
		this.joinType = joinType;		
	}

	public void setOnExp(ZExp onExp) {
		this.onExp = onExp;
	}

	public String getJoinType() {
		return this.joinType;
	}

	public ZExp getOnExp() {
		return this.onExp;
	}

	public String print(boolean withAlias) {
		String result;
		if(withAlias) {
			result = this.toString();
		} else {
			String alias = this.getAlias();
			if(alias == null || alias.equals("")) {
				result = this.toString();
			} else {
				this.setAlias("");
				result = this.toString();
				this.setAlias(alias);
			}
		}
		return result;
	}


	public void setDistinct(boolean distinct) {
		// TODO Auto-generated method stub
		
	}

	public boolean getDistinct() {
		// TODO Auto-generated method stub
		return false;
	}

	public void pushProjectionsDown(Collection<ZSelectItem> pushedProjections) {
		String unionQueryAlias = this.getAlias();
		
		for(SQLQuery sqlQuery : this.unionQueries) {
			String sqlQueryAlias = sqlQuery.getAlias();
			sqlQuery.setAlias(unionQueryAlias);
			sqlQuery.pushProjectionsDown(pushedProjections);
			if(sqlQueryAlias != null) {
				sqlQuery.setAlias(sqlQueryAlias);	
			}
			
			
//			Map<String, ZSelectItem> mapInnerAliasSelectItem = 
//					SQLQuery.buildMapAliasSelectItemAux(this.getAlias(), sqlQuery.getSelectItems());
//
//			Collection<ZSelectItem> newProjections = sqlQuery.pushProjectionsDown(pushedProjections
//					, mapInnerAliasSelectItem);
//			sqlQuery.setSelectItems(newProjections);
//			result.addAll(newProjections);
		}
		
	}

//	public Map<String, ZSelectItem> buildMapAliasSelectItem() {
//		Map<String, ZSelectItem> mapAliasSelectItem = new LinkedHashMap<String, ZSelectItem>();
//		for(SQLQuery sqlQuery : this.unionQueries) {
//			Map<String, ZSelectItem> mapAliasSelectItemAux = 
//					sqlQuery.buildMapAliasSelectItemAux(this.getAlias());
//			mapAliasSelectItem.putAll(mapAliasSelectItemAux);
//		}
//		return mapAliasSelectItem;
//	}

	public void pushOrderByDown(Collection<ZSelectItem> pushedProjections) {
		if(this.orderByConditions != null) {
			Map<String, ZSelectItem> mapInnerAliasSelectItem = new HashMap<String, ZSelectItem>();
			
			for(ZSelectItem selectItem : pushedProjections) {
				String selectItemColumn = selectItem.getColumn();
				mapInnerAliasSelectItem.put(selectItemColumn, selectItem);
			}
			
//			Collection<ZSelectItem> unionSelectItems = this.getSelectItems();
//			Map<String, ZSelectItem> mapInnerAliasSelectItem = 					
//					SQLQuery.buildMapAliasSelectItemAux(this.getAlias(), unionSelectItems);
			
//			Vector<ZOrderBy> newOrderByCollection = new Vector<ZOrderBy>();
//			for(ZOrderBy oldOrderBy : this.orderByConditions) {
//				ZExp orderByExp = oldOrderBy.getExpression();
//				ZExp newOrderByExp = MorphSQLUtility.replaceExp(orderByExp, whereReplacement);
//				ZOrderBy newOrderBy = new ZOrderBy(newOrderByExp);
//				newOrderBy.setAscOrder(oldOrderBy.getAscOrder());
//				newOrderByCollection.add(newOrderBy);
//			}
			
			Vector<ZOrderBy> newOrderByCollection = MorphSQLUtility.pushOrderByDown(this.orderByConditions
					, mapInnerAliasSelectItem);
			this.setOrderBy(newOrderByCollection);			
		}

	}

	public void pushFilterDown(ZExp pushedFilter) {
		Iterator<SQLQuery> it = this.unionQueries.iterator();
		while(it.hasNext()) {
			SQLQuery sqlQuery = it.next();
			Map<String, ZSelectItem> mapInnerAliasSelectItem = 
					SQLQuery.buildMapAliasSelectItemAux(this.getAlias(), sqlQuery.getSelectItems());
			ZExp newExpression = sqlQuery.pushExpDown(pushedFilter, mapInnerAliasSelectItem);
			sqlQuery.addWhere(newExpression);
		}
	}

	public void setSlice(long slice) {
		this.slice = slice;
		
	}

	public void setOffset(long offset) {
		this.offset = offset;
	}

	public void addSelectItems(Collection<ZSelectItem> newSelectItems) {
		for(SQLQuery query : this.unionQueries) {
			query.addSelectItems(newSelectItems);
		}
		
	}

	public void addGroupBy(ZGroupBy groupBy) {
		this.groupBy = groupBy;
	}

	public ZGroupBy getGroupBy() {
		return this.groupBy;
	}

	public void setGroupBy(ZGroupBy groupBy) {
		this.groupBy = groupBy;
		
	}

	public void pushGroupByDown() {
		//TODO
	}

	@Override
	public void setDbType(String dbType) {
		// TODO Auto-generated method stub
		
	}

}
