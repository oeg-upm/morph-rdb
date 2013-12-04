package es.upm.fi.dia.oeg.obdi.core.sql;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Vector;

import Zql.ZExp;
import Zql.ZGroupBy;
import Zql.ZOrderBy;
import Zql.ZSelectItem;

public interface IQuery extends SQLLogicalTable {
	public void setDatabaseType(String dbType);
	public String getDatabaseType();
	
	public String generateAlias();
	
	public void setSelectItems(Collection<ZSelectItem> newSelectItems);
	public Collection<ZSelectItem> getSelectItems();
	public LinkedList<String> getSelectItemAliases();
	public void cleanupSelectItems();
	public void addSelectItems(Collection<ZSelectItem> newSelectItems);
	
	public void cleanupOrderBy();
	public void setOrderBy(Vector<ZOrderBy> orderByConditions);
	public Vector<ZOrderBy> getOrderBy();
	
	public void setGroupBy(ZGroupBy groupBy);
	public ZGroupBy getGroupBy();
	public void addGroupBy(ZGroupBy groupBy);
	
	
	public void addWhere(ZExp newWhere);
	
	public void setDistinct(boolean distinct);
	public void setSlice(long slice);
	public void setOffset(long offset);
	public boolean getDistinct();

//	public IQuery removeSubQuery() throws Exception;
//	public IQuery removeSubQuery(Collection<ZSelectItem> newSelectItems
//			, ZExpression newWhereCondition, Vector<ZOrderBy> orderByConditions
//			, String databaseType) throws Exception;

	//public Map<String, ZSelectItem> buildMapAliasSelectItem();
	public void pushProjectionsDown(Collection<ZSelectItem> pushedProjections);
	public void pushFilterDown(ZExp pushedFilter);
	public void pushOrderByDown(Collection<ZSelectItem> pushedProjections);
	public void pushGroupByDown();
}
