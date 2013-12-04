package es.upm.fi.dia.oeg.obdi.core.sql;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Vector;

import Zql.ZConstant;
import Zql.ZExp;
import Zql.ZExpression;
import Zql.ZFromItem;
import Zql.ZOrderBy;
import Zql.ZSelectItem;

import com.hp.hpl.jena.graph.Node;

import es.upm.fi.dia.oeg.morph.base.Constants;
import es.upm.fi.dia.oeg.morph.base.MorphSQLUtility;
import es.upm.fi.dia.oeg.upm.morph.sql.MorphSQLConstant;
import es.upm.fi.dia.oeg.upm.morph.sql.MorphSQLSelectItem;

public class SQLUtility {

	private static ZExpression combineExpresions(ZExp exp1
			, Collection<? extends ZExp> exps, String logicalOperator
			) {
		Collection<ZExp> expressions = new Vector<ZExp>();
		if(exp1 != null) {
			expressions.add(exp1);	
		}
		
		if(exps != null) {
			expressions.addAll(exps);	
		}
		
		ZExpression result = SQLUtility.combineExpresions(expressions, logicalOperator);
		return result;
	}

		
	private static ZExpression combineExpresions(
			Collection<? extends ZExp> exps, String logicalOperator
			) {
		Iterator<? extends ZExp> it = exps.iterator();
		ZExpression result = null;

		if(exps.size() == 1) {
			ZExp exp = it.next();
			if(exp instanceof ZExpression) {
				result = (ZExpression) exp;
			} else {
				result = new ZExpression(it.next().toString());	
			}
		} else if(exps.size() > 1) {
			result = new ZExpression("AND");
			while(it.hasNext()) {
				result.addOperand(it.next());
			}
		}

		return result;
	}

//	public static ZExpression combineExpressions(ZExp exp1, ZExp exp2,
//			String logicalOperator) {
//		ZExpression result;
//		
//		if(exp1 == null && exp2 == null) {
//			result = null;
//		} else if(exp1 == null) {
//			if(exp2 instanceof ZExpression) {
//				result = (ZExpression) exp2; 
//			} else {
//				result = new ZExpression(exp2.toString());	
//			}
//		} else if (exp2 == null) {
//			if(exp1 instanceof ZExpression) {
//				result = (ZExpression) exp1;
//			} else {
//				result = new ZExpression(exp1.toString());	
//			}
//		} else {
//			result = new ZExpression(logicalOperator, exp1, exp2);	
//		}
//		
//		return result;
//	}

	private boolean isSubqueryEliminationPossible(SQLLogicalTable leftTable, SQLLogicalTable rightTable) {
		if(leftTable instanceof SQLFromItem) {
		} else if(leftTable instanceof SQLQuery) {
			SQLQuery leftTableSQLQuery = (SQLQuery) leftTable;
			Vector<ZFromItem> fromItems = leftTableSQLQuery.getFrom();
			for(ZFromItem fromItem : fromItems) {
				if(!(fromItem instanceof SQLFromItem)) {
				}
			}
		} else {
			
		}
			
		return false;
	}
	
	private IQuery joinQuery(Collection<ZSelectItem> selectItems, IQuery leftTable, IQuery rightTable, String joinType, ZExpression onExpression) {
		
		return null;
	}
	
	
	
	private static Collection<ZSelectItem> getSelectItemsByAlias(Collection<ZSelectItem> selectItems, String alias) {
		Collection<ZSelectItem> result = new Vector<ZSelectItem>();
		
		for(ZSelectItem selectItem : selectItems) {
			String selectItemAlias = selectItem.getAlias();
			if(alias.equalsIgnoreCase(selectItemAlias)) {
				result.add(selectItem);
			}
		}
		return result;
	}
	
	private static String getValueWithoutAlias(ZSelectItem selectItem) {
		String result;
		
		String selectItemAlias = selectItem.getAlias();
		if(selectItemAlias != null && !selectItemAlias.equals("")) {
			selectItem.setAlias("");
			result = selectItem.toString();
			selectItem.setAlias(selectItemAlias);
		} else {
			result = selectItem.toString();
		}
		return result;
	}
	
	private static Vector<ZOrderBy> pushOrderByDown(Collection<ZOrderBy> oldOrderByCollection,
			Map<String, ZSelectItem> mapInnerAliasSelectItem) {
		Map<ZConstant, ZConstant> whereReplacement = new LinkedHashMap<ZConstant, ZConstant>();
		for(String alias : mapInnerAliasSelectItem.keySet()) {
			ZSelectItem selectItem = mapInnerAliasSelectItem.get(alias);
			
			ZConstant aliasColumn = new ZConstant(alias, ZConstant.COLUMNNAME);
			
			String dbType = null;
			if(selectItem instanceof MorphSQLSelectItem) {
				dbType = ((MorphSQLSelectItem) selectItem).dbType();
			}

			
			MorphSQLConstant zConstant;
			if(selectItem.isExpression()) {
				String selectItemValue = selectItem.getExpression().toString();
				zConstant = MorphSQLConstant.apply(selectItemValue, ZConstant.UNKNOWN, dbType, null);
			} else {
				String selectItemAlias = selectItem.getAlias();
				if(selectItemAlias != null && !selectItemAlias.equals("")) {
					zConstant = MorphSQLConstant.apply(selectItemAlias, ZConstant.COLUMNNAME, dbType, null);
				} else {
					String selectItemValue;
					String selectItemTable = selectItem.getTable();
					String selectItemColumn = selectItem.getColumn();
					
					if(selectItemTable != null && !selectItemTable.equals("")) {
						selectItemValue = selectItemTable + "." + selectItemColumn;  
					} else {
						selectItemValue = selectItemColumn; 
					}
					zConstant = MorphSQLConstant.apply(selectItemValue, ZConstant.COLUMNNAME, dbType, null);
				}
			}
			whereReplacement.put(aliasColumn, zConstant);
		}

		Vector<ZOrderBy> newOrderByCollection = new Vector<ZOrderBy>();
		for(ZOrderBy oldOrderBy : oldOrderByCollection) {
			ZExp orderByExp = oldOrderBy.getExpression();
			ZExp newOrderByExp = MorphSQLUtility.replaceExp(orderByExp, whereReplacement);
			ZOrderBy newOrderBy = new ZOrderBy(newOrderByExp);
			newOrderBy.setAscOrder(oldOrderBy.getAscOrder());
			newOrderByCollection.add(newOrderBy);
		}
		return newOrderByCollection;
		//this.setOrderBy(newOrderByCollection);
	}
	
	private static boolean areAllConstants(Collection<ZExp> exps) {
		boolean result;
		
		if(exps == null) {
			result = false;
		} else {
			result = true;
			for(ZExp exp : exps) {
				if(!(exp instanceof ZConstant)) {
					result = false;
				}
			}			
		}
		
		return result;
	}

	private static Collection<ZExpression> containedInPrefix(ZExp exp, String prefix) {
		Collection<String> prefixes = new Vector<String>();
		prefixes.add(prefix);
		Collection<ZExpression> result = SQLUtility.containedInPrefixes(exp, prefixes, true);
		return result;
	}

		
	private static Collection<ZExpression> containedInPrefixes(
			ZExp exp, Collection<String> prefixes, boolean allPrefixes) {
		Collection<ZExpression> result = new HashSet<ZExpression>();
		
		if(exp instanceof ZExpression) {
			ZExpression expExpression = (ZExpression) exp;
			Vector<ZExp> operands = expExpression.getOperands();
			if(SQLUtility.areAllConstants(operands)) {
				Collection<ZExp> resultAux = new Vector<ZExp>();
				for(ZExp operand : operands) {
					if(operand instanceof ZConstant) {
						ZConstant zConstant = (ZConstant) operand;  
						if(zConstant.getType() == ZConstant.COLUMNNAME) {
							String operandString = MorphSQLUtility.printWithoutEnclosedCharacters(operand.toString());
							boolean found = false;
							for(String prefix : prefixes) {
								if(operandString.contains(prefix + ".") && !found) {
									resultAux.add(operand);	
								}						
							}
						} else {
							resultAux.add(operand);
						}
					}
				}
				
				if(allPrefixes) {
					if(resultAux.size() == operands.size()) {
						result.add(expExpression);
					}					
				} else {
					if(resultAux.size() > 0) {
						result.add(expExpression);
					}
				}
			} else {
				for(ZExp operand : operands) {
					Collection<ZExpression> resultChild = SQLUtility.containedInPrefixes(operand, prefixes, allPrefixes);
					result.addAll(resultChild);
				}
			}
		}
		
		return result;
	}
	
	private static Collection <ZSelectItem> getSelectItemsMapPrefix(Collection <ZSelectItem> selectItems) {
		Collection<ZSelectItem> result = new Vector<ZSelectItem>();

		for(ZSelectItem selectItem : selectItems) {
			String alias = selectItem.getAlias();
			if(alias.startsWith(Constants.PREFIX_MAPPING_ID())) {
				result.add(selectItem);
			}
		}
		
		return result;
	}
	
	private static Collection <ZSelectItem> getSelectItemsMapPrefix(
			Collection <ZSelectItem> selectItems, Node node, String pPrefix, String dbType) {
		Collection<ZSelectItem> result = new Vector<ZSelectItem>();
		String varNamePrefixed = Constants.PREFIX_MAPPING_ID() + node.getName();
		String prefix = pPrefix;
		if(!prefix.endsWith(".")) {
			prefix += ".";
		}
		
		for(ZSelectItem selectItem : selectItems) {
			String alias = selectItem.getAlias();
			
			if(alias != null && !alias.equals("")) {
				if(varNamePrefixed.equals(alias)) {
//					result.add(selectItem);
					
					ZSelectItem newSelectItem;
					if(selectItem.isExpression()) {
						newSelectItem = selectItem;
					} else {
						String selectItemColumnName = selectItem.getColumn();
						newSelectItem = MorphSQLSelectItem.apply(selectItemColumnName, prefix, dbType);
					}
					newSelectItem.setAlias(alias);
					result.add(newSelectItem);
				}				
			} else {
				String selectItemString = MorphSQLUtility.printWithoutEnclosedCharacters(
						selectItem.toString());
				
				if(varNamePrefixed.equals(selectItemString)) {
//					result.add(selectItem);
					
					ZSelectItem newSelectItem;
					if(selectItem.isExpression()) {
						newSelectItem = selectItem;
					} else {
						String selectItemColumnName = selectItem.getColumn();
						newSelectItem = MorphSQLSelectItem.apply(selectItemColumnName, prefix, dbType);
					}
					result.add(newSelectItem);
				}				
			}
		}
		
		return result;
	}
	
	private static void setDefaultAlias(Collection<ZSelectItem> selectItems) {
		for(ZSelectItem selectItem : selectItems) {
			String alias = selectItem.getAlias();
			if(alias == null || alias.equals("")) {
				alias = selectItem.getColumn();
				if(alias != null && !alias.equals("")) {
					selectItem.setAlias(alias);
				}
			}
		}
	}
}
