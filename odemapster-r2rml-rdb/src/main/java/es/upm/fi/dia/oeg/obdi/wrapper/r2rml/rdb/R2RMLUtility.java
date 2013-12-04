package es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb;


import java.io.ByteArrayInputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import Zql.ZConstant;
import Zql.ZExpression;
import Zql.ZQuery;
import Zql.ZStatement;
import Zql.ZqlParser;
import es.upm.fi.dia.oeg.morph.base.RegexUtility;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLQuery;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLJoinCondition;

public class R2RMLUtility {
	private static Logger logger = Logger.getLogger(R2RMLUtility.class);

	//private ConfigurationProperties configurationProperties;
	
	public static void main(String args[]) {
		String template = "Hello {Name} Please find attached {Invoice Number} which is due on {Due Date}";
		
		Map<String, String> replacements = new HashMap<String, String>();
		replacements.put("Name", "Freddy");
		replacements.put("Invoice Number", "INV0001");
		
		Collection<String> attributes = RegexUtility.getTemplateColumns(template, true);
		System.out.println("attributes = " + attributes);
		
		String template2 = R2RMLUtility.replaceTokens(template, replacements);
		System.out.println("template2 = " + template2);
		
	}
	
	
//	public static Collection<String> getAttributesFromStringTemplate2(String text) {
//		Collection<String> attributes = new HashSet<String>();
//		
//		Pattern pattern = Pattern.compile("\\{(.+?)\\}");
//		Matcher matcher = pattern.matcher(text);
//		while (matcher.find()) {
//			String matcherGroup1 = matcher.group(1);
//			attributes.add(matcherGroup1);
//		}
//		return attributes;		
//	}
	
	public static String replaceTokens(Matcher matcher
			, String text, Map<String, String> replacements) {
		StringBuffer buffer = new StringBuffer();
		while (matcher.find()) {
			String matcherGroup1 = matcher.group(1);
			String replacement = replacements.get(matcherGroup1);
			if (replacement != null) {
				matcher.appendReplacement(buffer, "");
				buffer.append(replacement);
			}
		}
		matcher.appendTail(buffer);
		return buffer.toString();		
	}
	
	public static String replaceTokens(String text,
			Map<String, String> replacements) {
		Pattern pattern = Pattern.compile("\\{(.+?)\\}");
		Matcher matcher = pattern.matcher(text);
		
		return R2RMLUtility.replaceTokens(matcher, text, replacements);
	}
	
	public static SQLQuery toSQLQuery(String sqlString) {
		ZQuery zQuery = R2RMLUtility.toZQuery(sqlString);
		SQLQuery sqlQuery = new SQLQuery(zQuery);
		return sqlQuery;
	}
	
	
	public static ZQuery toZQuery(String sqlString) {
		try {
			//sqlString = sqlString.replaceAll(".date ", ".date2");
			ByteArrayInputStream bs = new ByteArrayInputStream(sqlString.getBytes());
			ZqlParser parser = new ZqlParser(bs);
			ZStatement statement = parser.readStatement();
			ZQuery zQuery = (ZQuery) statement;
			
			return zQuery;
		} catch(Exception e) {
			//e.printStackTrace();
			logger.error("error parsing query string : \n" + sqlString);
			logger.error("error message = " + e.getMessage());
			return null;
		}
	}

//	public static Collection<ZSelectItem> getDatabaseColumns(R2RMLTriplesMap triplesMap) {
//		Collection<ZSelectItem> result = new HashSet<ZSelectItem>();
//		
//		R2RMLSubjectMap subjectMap = triplesMap.getSubjectMap();
//		if(subjectMap != null) {
//			Collection<ZSelectItem> subjectMapColumns = R2RMLUtility.getDatabaseColumns(subjectMap);
//			if(subjectMapColumns != null) { result.addAll(subjectMapColumns); }
//			
//		}
//		
//		Collection<R2RMLPredicateObjectMap> predicateObjectMaps = triplesMap.getPredicateObjectMaps();
//		if(predicateObjectMaps != null) {
//			for(R2RMLPredicateObjectMap predicateObjectMap : predicateObjectMaps) {
//				R2RMLPredicateMap predicateMap = predicateObjectMap.getPredicateMap();
//				Collection<ZSelectItem> predicateMapColumns = R2RMLUtility.getDatabaseColumns(predicateMap);
//				if(predicateMapColumns != null) { result.addAll(predicateMapColumns);}
//				
//				R2RMLObjectMap objectMap = predicateObjectMap.getObjectMap();
//				Collection<ZSelectItem> objectMapColumns = R2RMLUtility.getDatabaseColumns(objectMap);
//				if(objectMapColumns != null) { result.addAll(objectMapColumns);}
//
//				R2RMLRefObjectMap refObjectMap = predicateObjectMap.getRefObjectMap();
//				Collection<ZSelectItem> refObjectMapColumns = R2RMLUtility.getDatabaseColumns(refObjectMap);
//				if(refObjectMapColumns != null) { result.addAll(refObjectMapColumns);}
//
//			}
//		}
//		
//		return result;
//	}
	
//	private static Collection<ZSelectItem> getDatabaseColumns(
//			R2RMLRefObjectMap refObjectMap) {
//		// TODO Auto-generated method stub
//		return null;
//	}


//	public static Collection<ZSelectItem> getDatabaseColumns(R2RMLTermMap termMap) {
//		Collection<ZSelectItem> result = new HashSet<ZSelectItem>();
//
//		if(termMap.getTermMapType() == TermMapType.CONSTANT) {
//			ZSelectItem selectItem = new ZSelectItem();
//			selectItem.setExpression(new ZConstant(termMap.getOriginalValue(), ZConstant.UNKNOWN));
//			result.add(selectItem);
//		} else if(termMap.getTermMapType() == TermMapType.COLUMN) {
//			ZSelectItem selectItem = new ZSelectItem(termMap.getOriginalValue());
//			result.add(selectItem);
//		} else if(termMap.getTermMapType() == TermMapType.TEMPLATE) {
//			String template = termMap.getOriginalValue();
//			//Collection<String> attributes = R2RMLUtility.getAttributesFromStringTemplate(template);
//			RegexUtility regexUtility = new RegexUtility();
//			Collection<String> attributes = regexUtility.getTemplateColumns(template, true);
//			if(attributes != null) {
//				for(String attribute : attributes) {
//					ZSelectItem selectItem = new ZSelectItem(attribute);
//					result.add(selectItem);
//				}
//			}
//		}
//
//		return result;
//	}
	

	
	
	public static ZExpression generateJoinCondition(Collection<R2RMLJoinCondition> joinConditions
			, String parentTableAlias, String joinQueryAlias, String dbType) {
		ZExpression onExpression = null;

		
		if(joinConditions != null) {
			for(R2RMLJoinCondition joinCondition : joinConditions) {
				//String childColumnName = logicalTableAlias + "." + joinCondition.getChildColumnName();
				String childColumnName = joinCondition.getChildColumnName();
				//SQLSelectItem childSelectItem = new SQLSelectItem(childColumnName);  
//				SQLSelectItem childSelectItem = SQLSelectItem.createSQLItem(dbType, childColumnName, null);
				
//				String[] childColumnNameSplit = childColumnName.split("\\.");
//				if(childColumnNameSplit.length == 1) {
//					childColumnName = parentTableAlias + "." + childColumnName; 
//				}
				
				childColumnName = parentTableAlias + "." + childColumnName;
				ZConstant childColumn = new ZConstant(childColumnName, ZConstant.COLUMNNAME);

				 
				String parentColumnName = joinCondition.getParentColumnName();
//				SQLSelectItem parentSelectItem = SQLSelectItem.createSQLItem(dbType, parentColumnName, null);
				parentColumnName = joinQueryAlias + "." + parentColumnName;
				ZConstant parentColumn = new ZConstant(parentColumnName, ZConstant.COLUMNNAME);
				
				ZExpression joinConditionExpression = new ZExpression("=", childColumn, parentColumn);
				if(onExpression == null) {
					onExpression = joinConditionExpression;
				} else {
					onExpression = new ZExpression("AND", onExpression, joinConditionExpression);
				}
			}
		}
		
		return onExpression;
	}

}
