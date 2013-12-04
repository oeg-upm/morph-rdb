package es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;

import es.upm.fi.dia.oeg.morph.base.ColumnMetaData;
import es.upm.fi.dia.oeg.morph.base.Constants;
import es.upm.fi.dia.oeg.morph.base.TableMetaData;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractLogicalTable;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLFromItem.LogicalTableType;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.engine.R2RMLElement;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.engine.R2RMLElementVisitor;
//import scala.reflect.generic.Trees.This;

public abstract class R2RMLLogicalTable extends AbstractLogicalTable implements R2RMLElement {
	private static Logger logger = Logger.getLogger(R2RMLLogicalTable.class);
	
	LogicalTableType logicalTableType;
	private String alias;
	private R2RMLTriplesMap owner;
	
	R2RMLLogicalTable(R2RMLTriplesMap owner) {this.owner = owner;}

	public void buildMetaData(Connection conn) throws Exception {
		if(conn != null) {
			try {
				Map<String, TableMetaData> tablesMetaData = 
						owner.getOwner().getTablesMetaData();

				String tableName = null; 
				if(this instanceof R2RMLTable) {
					R2RMLTable r2rmlTable = (R2RMLTable) this;
					tableName = r2rmlTable.getValue();
				} else if (this instanceof R2RMLSQLQuery){
					R2RMLSQLQuery r2rmlSQLQuery = (R2RMLSQLQuery) this;
					tableName = "(" + r2rmlSQLQuery.getValue() + ")";
				}
				
				TableMetaData tableMetaData = tablesMetaData.get(tableName);
				if(tableMetaData == null) {
					logger.info("building table metadata for " + tableName);

					java.sql.Statement stmt = conn.createStatement();
					String query = "SELECT COUNT(*) FROM " + tableName + " T";
					ResultSet rs = stmt.executeQuery(query);
					rs.next();
					long tableRows = rs.getLong(1);
					tableMetaData = new TableMetaData(tableName, tableRows);
					tablesMetaData.put(tableName, tableMetaData);					
				}
				super.tableMetaData = tableMetaData;
			} catch(Exception e) {
				logger.error("Error while getting size of logical table " + this);
				throw e;
			}

			try {
				Map<String, Map<String, ColumnMetaData>> mapTableColumnsMetaData = 
						owner.getOwner().getColumnsMetaData();
				
				String mapTableColumnsMetaDataKey = null;
				String query = null;
				if(this instanceof R2RMLTable) {
					R2RMLTable r2rmlTable = (R2RMLTable) this;
					mapTableColumnsMetaDataKey = r2rmlTable.getValue();
					query = "SELECT * FROM " + mapTableColumnsMetaDataKey + " WHERE 1=0";
				} else if (this instanceof R2RMLSQLQuery){
					R2RMLSQLQuery r2rmlSQLQuery = (R2RMLSQLQuery) this;
					mapTableColumnsMetaDataKey = r2rmlSQLQuery.getValue();
					query = r2rmlSQLQuery.getValue();
				}

				Map<String, ColumnMetaData> columnsMetaData = mapTableColumnsMetaData.get(mapTableColumnsMetaDataKey);
				if(columnsMetaData == null) {
					columnsMetaData = new HashMap<String, ColumnMetaData>();
					mapTableColumnsMetaData.put(mapTableColumnsMetaDataKey, columnsMetaData);
					
					logger.info("building column metadata");
					java.sql.Statement stmt = conn.createStatement();
					ResultSet rs = stmt.executeQuery(query);
					ResultSetMetaData rsmd = rs.getMetaData();
					int columnCount = rsmd.getColumnCount();
					for(int i=1; i<=columnCount; i++) {
						String columnName = rsmd.getColumnName(i);
						String columnTypeName = rsmd.getColumnTypeName(i);
						ColumnMetaData columnMetaData = new ColumnMetaData(
								mapTableColumnsMetaDataKey, columnName, columnTypeName, true);
						columnsMetaData.put(columnName, columnMetaData);
					}
					mapTableColumnsMetaData.put(mapTableColumnsMetaDataKey, columnsMetaData);
				}
				super.columnsMetaData = columnsMetaData;
			} catch(Exception e) {
				logger.error("Error while producing ResultSetMetaData for Logical Table of Triples Map " + owner);
			}

		}
	}

	static R2RMLLogicalTable parse(Resource resource, R2RMLTriplesMap owner) throws Exception {
		
		R2RMLLogicalTable logicalTable = null; 
		Statement tableNameStatement = resource.getProperty(
				Constants.R2RML_TABLENAME_PROPERTY());
		if(tableNameStatement != null) {
			String tableName = tableNameStatement.getObject().toString();
			logicalTable = new R2RMLTable(tableName, owner);
		} else {
			Statement sqlQueryStatement = resource.getProperty(
					Constants.R2RML_SQLQUERY_PROPERTY());
			if(sqlQueryStatement == null) {
				logger.error("Invalid logical table defined : " + resource);
			}
			String sqlQueryString = sqlQueryStatement.getObject().toString().trim();
			logicalTable = new R2RMLSQLQuery(sqlQueryString, owner);
		}

		return logicalTable;
	}

	public abstract String getValue();

	@Override
	public String toString() {
		String result = "";
		if(this instanceof R2RMLTable) {
			result = "R2RMLTable";
		} else if(this instanceof R2RMLSQLQuery) {
			result = "R2RMLSQLQuery";
		}

		return result + ":" + this.getValue();
	}

	public LogicalTableType getLogicalTableType() {
		return logicalTableType;
	}

	public Object accept(R2RMLElementVisitor visitor) {
		Object result = visitor.visit(this);
		return result;
	}

	public String getAlias() {
		return alias;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}







}
