package es.upm.fi.dia.oeg.obdi.core;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import Zql.ZConstant;
import Zql.ZExp;
import Zql.ZExpression;

import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.ontology.OntModelSpec;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.shared.CannotEncodeCharacterException;

import es.upm.fi.dia.oeg.morph.base.Constants;
import es.upm.fi.dia.oeg.upm.morph.sql.MorphSQLConstant;

public class ODEMapsterUtility {
	private static Logger logger = Logger.getLogger(ODEMapsterUtility.class);
	//private ConfigurationProperties configurationProperties;
	
	public static boolean inArray(String[] delegableOperations, String operationId) {
		boolean isDelegableOperation = false;

		for(int i=0 ; i<delegableOperations.length && !isDelegableOperation; i++) {
			if(delegableOperations[i].trim().equalsIgnoreCase(operationId.trim())) {
				isDelegableOperation = true;
			}
		}

		return isDelegableOperation;
	}

	public static String readFileAsString(String filePath) throws IOException{
		byte[] buffer = new byte[(int) new File(filePath).length()];
		BufferedInputStream f = null;
		try {
			f = new BufferedInputStream(new FileInputStream(filePath));
			f.read(buffer);
		} catch(IOException e) {
			logger.error(e.getMessage());
			throw e;
		} finally {
			if (f != null) try { f.close(); } catch (IOException ignored) { }
		}
		return new String(buffer);
	}

	public static List<String> readFileAsLines(String aFile) throws IOException{
		BufferedReader input =  new BufferedReader(new FileReader(aFile));
		List<String> lines = new Vector<String>();
		try {
			String line = null; //not declared within while loop
			/*
			 * readLine is a bit quirky :
			 * it returns the content of a line MINUS the newline.
			 * it returns null only for the END of the stream.
			 * it returns an empty String if two newlines appear in a row.
			 */
			while (( line = input.readLine()) != null){
				lines.add(line);
			}
		}
		finally {
			input.close();
		}
		return lines;
	}

	public static OntModel openOntoDescFromFile(String filePath) {
		logger.debug("opening from file " + filePath + "..");
		OntModel m = ModelFactory.createOntologyModel(
				OntModelSpec.OWL_DL_MEM, null);
		try {
			File theOntoDoc = new File(filePath);
			FileInputStream fis = new FileInputStream(theOntoDoc);
			m.read(fis, null);
			return m;

		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return m;
		}
	}

	public static OntModel openOntoDescFromUrl(String url) {
		logger.debug("opening from url " + url + "..");
		OntModel m = ModelFactory.createOntologyModel(
				OntModelSpec.OWL_DL_MEM, null);
		m.read(url);
		//m.write(System.out);
		return m;

	}

	public static String getCountryByURI(String sUri) {
		try {
			URI uri = new URI(sUri);
			String uriHost = uri.getHost();

			int sURILastDot = uriHost.lastIndexOf(".");
			String sCountryDomain = uriHost.substring(sURILastDot + 1);
			return sCountryDomain;
		} catch (URISyntaxException e) {
			e.printStackTrace();
			return null;
		}
	}


	public static String encodeLiteral(String originalLiteral) {
		String result = originalLiteral;
		try {
			if(result != null) {
				result = result.replaceAll("\\\\", "/");
				result = result.replaceAll("\"", "%22");
				result = result.replaceAll("\\\\n", " ");
				result = result.replaceAll("\\\\r", " ");
				result = result.replaceAll("\\\\ ", " ");
				result = result.replaceAll("_{2,}+", "_");
				result = result.replaceAll("\n","");
				result = result.replaceAll("\r", "");
				result = result.replace("\\ ", "/");
			}
		} catch(Exception e) {
			logger.error("Error encoding literal for literal = " + originalLiteral + " because of " + e.getMessage());
		}

		return result;
	}

	public static String removeStrangeChars(String someString) {
		someString = someString.replaceAll("Ñ", "N");
		someString = someString.replaceAll("ñ", "n");
		someString = someString.replaceAll("á", "a");
		//someString = someString.replaceAll("�?", "A");
		someString = someString.replaceAll("ª", "a");
		someString = someString.replaceAll("ã", "a");
		someString = someString.replaceAll("Ã", "A");

		someString = someString.replaceAll("é", "e");
		someString = someString.replaceAll("É", "E");
		someString = someString.replaceAll("ë", "e");
		someString = someString.replaceAll("Ë", "E");
		someString = someString.replaceAll("í", "i");
		//someString = someString.replaceAll("�?", "I");
		someString = someString.replaceAll("ï", "i");
		//someString = someString.replaceAll("�?", "I");
		someString = someString.replaceAll("ó", "o");
		someString = someString.replaceAll("Ó", "O");
		someString = someString.replaceAll("ö", "o");
		someString = someString.replaceAll("Ö", "O");
		someString = someString.replaceAll("ú", "u");
		someString = someString.replaceAll("Ú", "U");
		someString = someString.replaceAll("ü", "u");
		someString = someString.replaceAll("Ü", "U");

		return someString;
	}

//	private static String preEncoding(String uri) {
//		uri = uri.replaceAll("\\(", "_");
//		uri = uri.replaceAll("\\)", "_");
//		uri = uri.replaceAll("\\[", "_");
//		uri = uri.replaceAll("\\]", "_");
//		//			uri = uri.replaceAll("\\.", "_");
//
//		uri = uri.replaceAll("\n", " ");
//		uri = uri.replaceAll("\\n", " ");
//		uri = uri.replaceAll("\t", " ");
//		uri = uri.replaceAll("\\t", " ");
//		uri = uri.replaceAll("\\r", " ");
//
//		//			uri = uri.replaceAll("\"", "_");
//
//		uri = uri.replaceAll("\\\\", "%5C");
//		uri = uri.replaceAll("\\b\\s{2,}\\b", " ");
//
//
//
//
//
//
//
//		return uri;
//
//	}

//	private static String postEncoding(String uri) {
//		uri = uri.replaceAll(",", "%2C");
//		uri = uri.replaceAll("&", "%26");
//		//uri = uri.replaceAll("&", "and");
//		uri = uri.replaceAll("'", "%27");
//		//uri = uri.replaceAll(" ", "%20");
//		uri = uri.replaceAll("_{2,}+", "_");
//		uri = uri.replaceAll("%23", "#");
//
//		//uri = uri.replaceAll("%20", "_");
//
//		//uri = uri.toLowerCase();
//
//		return uri;
//	}

	public static String encodeURI(String originalURI)  throws Exception {
		String uri = originalURI;
		try {
			uri = uri.trim();
			
			//uri = uri.replaceAll(" ", "%20");
			uri = uri.replaceAll(" ", "_");
			
			//uri = ODEMapsterUtility.removeStrangeChars(uri);

			//uri = ODEMapsterUtility.preEncoding(uri);

			//	uri = new URI(uri).toASCIIString();
			//uri = new URI(null, uri, null).toASCIIString();

			//uri = ODEMapsterUtility.postEncoding(uri);

		} catch(Exception e) {
			logger.error("Error encoding uri for uri = " + originalURI + " because of " + e.getMessage());
			throw e;
		}

		return uri;
	}

	public static void main(String args[]) throws Exception {

		String str1 = "[\nab,c";
		System.out.println("str1Encoded = " + ODEMapsterUtility.encodeURI(str1));

		String str2 = "_";
		System.out.println("str2Encoded = " + ODEMapsterUtility.encodeURI(str2));

		String str3 = "\\";
		System.out.println("str3Encoded = " + ODEMapsterUtility.encodeURI(str3));

		String str4 = "Ã�ndi%ce";
		System.out.println("str4Encoded = " + ODEMapsterUtility.encodeURI(str4));

		String str5 = "	?";
		System.out.println("str5Encoded = " + ODEMapsterUtility.encodeURI(str5));

		String str6 = ",";
		System.out.println("str6Encoded = " + ODEMapsterUtility.encodeURI(str6));

		String str7 = "'";
		System.out.println("str7Encoded = " + ODEMapsterUtility.encodeURI(str7));

		String str8 = "abc def";
		System.out.println("str8Encoded = " + ODEMapsterUtility.encodeURI(str8));

		String str9 = "\"";
		System.out.println("str9Encoded = " + ODEMapsterUtility.encodeURI(str9));

		String uri1 = "http://edu.linkeddata.es/UPM/resource/Actividad/Manual    de la calidad del Laboratorio de Ensayos QuÃ�micos Industriales , ((LEQIM)), Rev.10";
		System.out.println("uri1Encoded = " + ODEMapsterUtility.encodeURI(uri1));

		String uri2 = "http://edu.linkeddata.es/UPM/resource/LineaInvestigaci%C3%B3n/31656_AnÃ¡lisis del Sector de las TIC\\s";
		System.out.println("uri2Encoded = " + ODEMapsterUtility.encodeURI(uri2));

		String uri3 = "http://edu.linkeddata.es/UPM/resource/OtroParticipante/ Gallardo	_Fernando";
		System.out.println("uri3Encoded = " + ODEMapsterUtility.encodeURI(uri3));

		String uri4 = "http://www.google.com/espaÃ±a spain#lang=en,es";
		System.out.println("uri4Encoded = " + ODEMapsterUtility.encodeURI(uri4));

		String uri5 = "http://geo.linkeddata.es/HospitalesMadrid#HospitÃ¡l110051";
		System.out.println("uri5Encoded = " + ODEMapsterUtility.encodeURI(uri5));

		String uri6 = "http://edu.linkeddata.es/UPM/resource/Fecha/31/12/2004";
		System.out.println("uri6Encoded = " + ODEMapsterUtility.encodeURI(uri6));

		String uri7 = "http://edu.linkeddata.es/UPM/resource/OtroParticipante/LABORATORIO \"SALVADOR VELAYOS\"_INSTITUTO DE MAGNETISMO APLICADO";
		System.out.println("uri7Encoded = " + ODEMapsterUtility.encodeURI(uri7));

		String uri8 = "http://edu_linkeddata_es/UPM/resource/Actividad/10013_ANÃ�LISIS%20E%20INVESTIGACIÃ“N%20DE%20ACELERACIÃ“N%20DE\n%20VALORACIÃ“N%20FINANCIERA%20MEDIANTE%20PLATAFORMAS%20RECONFIGURABLES";
		System.out.println("uri8Encoded = " + ODEMapsterUtility.encodeURI(uri8));

		System.out.println("uri8Encoded = " + ODEMapsterUtility.encodeURI(uri8));


		String literal1 = "Say \\r \"Hello World\"";
		System.out.println("literal1 = " + literal1);
		System.out.println("literal1Encoded = " + ODEMapsterUtility.encodeLiteral(literal1));

		String literal2 = "Say \\n \'Hello World\'";
		System.out.println("literal2 = " + literal2);
		System.out.println("literal2Encoded = " + ODEMapsterUtility.encodeLiteral(literal2));

		String literal3 = "Soledad_____Hurtado";
		System.out.println("literal3Encoded = " + ODEMapsterUtility.encodeLiteral(literal3));

		String str10 = "A & D ARQUITECTURA Y DECORACIÓN 2000";
		System.out.println("str10Encoded = " + ODEMapsterUtility.encodeURI(str10));

		String str11 = "Hello / World #, How are \\ you?";
		System.out.println("str11 before encoded = " + str11);
		System.out.println("str11 = " + ODEMapsterUtility.encodeUnsafeChars(str11));
		System.out.println("str11 = " + ODEMapsterUtility.encodeReservedChars(str11));

		/*
		Connection conn = Utility.getLocalConnection("bsbm1m", "bsbm1m"
				, "nl.cwi.monetdb.jdbc.MonetDriver", "jdbc:monetdb://localhost/demo", null);
		String bsbmQuery01 = "SELECT distinct nr, label"
			+ " FROM product p, producttypeproduct ptp"
			+ " WHERE p.nr = ptp.product AND ptp.\"productType\"=105"
			+ " AND p.propertyNum1 > 486"
			+ "	AND p.nr IN (SELECT distinct product FROM productfeatureproduct WHERE productFeature=815)"
			+ "	AND p.nr IN (SELECT distinct product FROM productfeatureproduct WHERE productFeature=814)"
			+ " ORDER BY label"
			+ " LIMIT 10";
		Utility.executeQuery(conn, bsbmQuery01);
		 */
	}


	//Creates a triple
	public static String createTriple(String subject, String predicate, String object)
	{
		StringBuffer result = new StringBuffer();
		result.append(subject);
		result.append(" ");
		result.append(predicate);
		result.append(" ");
		result.append(object);
		result.append(" .\n");


		return result.toString();
	}

	//Creates a quad
	public static String createQuad(String subject, String predicate, String object, String graph)
	{
		StringBuffer result = new StringBuffer();
		result.append(subject);
		result.append(" ");
		result.append(predicate);
		result.append(" ");
		result.append(object);
		if(graph != null) {
			result.append(" ");
			result.append(graph);
		}
		result.append(" .\n");


		return result.toString();
	}

	//Create Literal
	public static String createLiteral(String value)
	{
		value = ODEMapsterUtility.encodeLiteral(value);
		StringBuffer result = new StringBuffer();
		result.append("\"");
		result.append(value);
		result.append("\"");
		return result.toString();
	}

	//Create typed literal
	public static String createDataTypeLiteral(String value, String datatypeURI)
	{
		value = ODEMapsterUtility.encodeLiteral(value);
		StringBuffer result = new StringBuffer();
		result.append("\"");
		result.append(value);
		result.append("\"^^");
		result.append("<" + datatypeURI + ">");
		return result.toString();
	}

	//Create language tagged literal
	public static String createLanguageLiteral(String text, String languageCode)
	{
		text = ODEMapsterUtility.encodeLiteral(text);
		StringBuffer result = new StringBuffer();
		result.append("\"");
		result.append(text);
		result.append("\"@");
		result.append(languageCode);
		return result.toString();
	}

	//Create URIREF from namespace and element
	public static String createURIref(String namespace, String element)
	{
		StringBuffer result = new StringBuffer();
		result.append("<");
		result.append(namespace);
		result.append(element);
		result.append(">");
		return result.toString();
	}

	//Create URIREF from URI
	public static String createURIref(String uri)
	{
		if(uri == null) {
			return null;
		} else {
			StringBuffer result = new StringBuffer();
			result.append("<");
			result.append(uri);
			result.append(">");
			return result.toString();			
		}

	}

	//Create blank node from id
	public static String createBlankNode(String id)
	{
		StringBuffer result = new StringBuffer();
		result.append("_:");
		result.append(id);
		return result.toString();
	}

	//TODO improve this
	public static boolean isIRI(String id) {
		if(id != null && id.startsWith("http://")) {
			return true;
		} else {
			return false;
		}
	}













	//	public static R2OAttributeMapping generatePKColumnAttributeMapping(R2OConceptMapping cm, String oldName, String newName) {
	//		R2OTransformationExpression uriAs = cm.getURIAs();
	//
	//		R2OColumnRestriction pkColumnRestriction = (R2OColumnRestriction) uriAs.getLastRestriction();
	//		R2ODatabaseColumn pkColumn = pkColumnRestriction.getDatabaseColumn();
	//		R2OTransformationExpression te = new R2OTransformationExpression(R2OConstants.TRANSFORMATION_OPERATOR_CONSTANT);
	//		R2ORestriction restriction = new R2OColumnRestriction(pkColumn);
	//		te.addRestriction(restriction);
	//		R2OSelector selector = new R2OSelector(null, te);
	//		String pkColumnAlias = cm.generatePKColumnAlias();
	//		R2OAttributeMapping pkAttributeMapping = new R2OAttributeMapping(pkColumnAlias);
	//		pkAttributeMapping.addSelector(selector);
	//		pkAttributeMapping.setMappedPKColumn(true);
	//
	//		return pkAttributeMapping;
	//	}

	public static boolean isInteger( String input )  
	{  
		try  
		{  
			Integer.parseInt( input );  
			return true;  
		}  
		catch( Exception e)  
		{  
			return false;  
		}  
	}
	
//	public ZSelectItem renameColumn(ZSelectItem selectItem, String tableName, String alias
//			, boolean matches, String databaseType) throws Exception {
//
//		ZSelectItem result;
//
//		boolean isExpression = selectItem.isExpression();
//		if(isExpression) {
//			ZExp selectItemExp = selectItem.getExpression();
//			ZExpression newExpression = ODEMapsterUtility.renameColumns((ZExpression) selectItemExp
//					, tableName, alias, matches, databaseType);
//			selectItem.setExpression(newExpression);
//			result = selectItem;
//		} else {
//			String selectItemSchema = selectItem.getSchema() ;
//			String selectItemTable = selectItem.getTable();
//
//			if(tableName.equals(selectItemSchema + "." + selectItemTable) == matches) {
//				ZSelectItem newSelectItem = new ZSelectItem(alias + "." + selectItem.getColumn());
//				if(selectItem.getAlias() != null) {
//					newSelectItem.setAlias(selectItem.getAlias());
//				}
//				result = newSelectItem;
//			} else {
//				result = selectItem;
//
//			}
//		}
//
//		return result;
//	}

//	public Collection<ZSelectItem> renameColumns(Collection<ZSelectItem> selectItems
//			, String tableName, String alias, boolean matches, String databaseType) throws Exception {
//		Collection<ZSelectItem> result = new Vector<ZSelectItem>();
//
//		for(ZSelectItem selectItem :selectItems) {
//			ZSelectItem renamedColumn = this.renameColumn(selectItem, tableName, alias, matches, databaseType);
//			result.add(renamedColumn);
//		}
//
//		return result;
//	}    

//	public static ZExpression renameColumns(ZExpression zExpression
//			, String oldName, String newName
//			, boolean matchCondition, String databaseType) throws Exception 
//			{
//		if(zExpression == null) {
//			return null;
//		}
//
//		String operator = zExpression.getOperator();
//		ZExpression result = new ZExpression(operator);
//
//		Collection<ZExp> operands = zExpression.getOperands();
//		for(ZExp operand : operands) {
//			if(operand instanceof ZConstant) {
//				//oldName = benchmark.dbo.product
//				//zExpression = benchmark.dbo.producttype.nr
//
//				ZConstant newOperandConstant;
//
//				ZConstant operandConstant = (ZConstant) operand;
//
//				if(operandConstant.getType() == ZConstant.COLUMNNAME) {
//					String operandConstantValue = operandConstant.getValue();
//					operandConstantValue = operandConstantValue.replaceAll("\'", "");
//					SQLSelectItem oldSelectItem = 
//							new SQLSelectItem(operandConstantValue);
//					if(oldSelectItem.getSchema() == null && oldSelectItem.getTable() == null) {
//						if(operandConstantValue.equalsIgnoreCase(oldName) == matchCondition) {
//							newOperandConstant = ODEMapsterUtility.constructDatabaseColumn(newName, databaseType);
//						} else {
//							newOperandConstant = operandConstant;
//						}
//					} else if(oldSelectItem.getSchema() != null && oldSelectItem.getTable() != null) {
//						String oldTableName = oldSelectItem.getSchema() + "." + oldSelectItem.getTable();
//
//						if(oldTableName.equalsIgnoreCase(oldName) == matchCondition) {
//							//String newOperandConstantValue = operandConstantValue.replaceAll(oldName, newName);
//							//newOperandConstant = Utility.constructDatabaseColumn(newOperandConstantValue);
//							newOperandConstant = ODEMapsterUtility.constructDatabaseColumn(newName + "." + oldSelectItem.getColumn(), databaseType);
//						} else {
//							newOperandConstant = operandConstant;
//						}
//
//					} else {
//						newOperandConstant = operandConstant;
//					}
//
//
//
//					//					ZAliasedName oldColumnName = new ZAliasedName(
//					//							operandConstantValue, ZAliasedName.FORM_COLUMN);
//					//					if(operandConstantValue.startsWith(oldName + ".") == matchCondition) {
//					//						//be careful here when passing sql server column names that have 4 elements 
//					//						//(db.schema.table.column)
//					//						String newColumnName = newName + "." + oldColumnName.getColumn();
//					//						//newOperandConstant = new ZConstant(newColumnName, operandConstant.getType());
//					//						newOperandConstant = Utility.constructDatabaseColumn(newColumnName);
//					//					} else if(operandConstantValue.equalsIgnoreCase(oldName)) {
//					//						newOperandConstant = Utility.constructDatabaseColumn(newName);
//					//					} else {
//					//						newOperandConstant = operandConstant;
//					//					}
//
//				} else {
//					newOperandConstant = operandConstant;					
//				}
//				result.addOperand(newOperandConstant);
//			} else if(operand instanceof ZExpression) {
//				ZExpression operandExpression  = (ZExpression) operand;
//				ZExpression renamedColumns = ODEMapsterUtility.renameColumns(operandExpression, oldName, newName, matchCondition, databaseType); 
//				result.addOperand(renamedColumns);
//			} else {
//				throw new Exception("Unsupported columns renaming operation!");
//			}
//		}
//		return result;
//			}

	public static ZConstant constructDatabaseColumn(String columnName, String databaseType) {
		
		if(databaseType == null) { databaseType = Constants.DATABASE_MYSQL(); }

		ZConstant zColumn;
		if(databaseType.equalsIgnoreCase(Constants.DATABASE_MONETDB())) {
			zColumn = MorphSQLConstant.apply(columnName, ZConstant.COLUMNNAME, databaseType, null);
		} else if(databaseType.equalsIgnoreCase(Constants.DATABASE_MYSQL())) {
			//			zColumn = new ZConstant("\'" + columnName + "\'", ZConstant.COLUMNNAME);
			zColumn = new ZConstant(columnName, ZConstant.COLUMNNAME);
		} else {
			zColumn = new ZConstant(columnName, ZConstant.COLUMNNAME);
		}

		return zColumn;
	}

	public ZConstant constructDatabaseColumn(String schema, String table, String column
			, String databaseType) {
		return ODEMapsterUtility.constructDatabaseColumn(schema + "." + table + "." + column, databaseType);
	}

	public static String replaceNameWithSpaceChars(String tableName) {
		tableName = tableName.trim();
		String result = tableName;

		String[] tableNameSplits = tableName.split(" ");
		if(tableNameSplits.length > 1) {
			result = "`" + tableName + "`"; 
		}

		return result;
	}

	public static String encodeUnsafeChars(String originalValue) {
		String result = originalValue; 
		if(result != null) {
			result = result.replaceAll("\\%", "%25");//put this first
			result = result.replaceAll("<", "%3C");
			result = result.replaceAll(">", "%3E");
			result = result.replaceAll("#", "%23");

			result = result.replaceAll("\\{", "%7B");
			result = result.replaceAll("\\}", "%7D");
			result = result.replaceAll("\\|", "%7C");
			result = result.replaceAll("\\\\", "%5C");
			result = result.replaceAll("\\^", "%5E");
			result = result.replaceAll("~", "%7E");
			result = result.replaceAll("\\[", "%5B");
			result = result.replaceAll("\\]", "%5D");
			result = result.replaceAll("`", "%60");
		}
		return result;
	}

	public static String encodeReservedChars(String originalValue) {
		String result = originalValue; 
		if(result != null) {
			result = result.replaceAll("\\$", "%24");
			result = result.replaceAll("&", "%26");
			result = result.replaceAll("\\+", "%2B");
			result = result.replaceAll(",", "%2C");
			result = result.replaceAll("/", "%2F");
			result = result.replaceAll(":", "%3A");
			result = result.replaceAll(";", "%3B");
			result = result.replaceAll("=", "%3D");
			result = result.replaceAll("\\?", "%3F");
			result = result.replaceAll("@", "%40");
		}
		return result;
	}


	protected static Pattern elementContentEntities = Pattern.compile( "<|>|&|[\0-\37&&[^\n\t]]|\uFFFF|\uFFFE" );
	/**
        Answer <code>s</code> modified to replace &lt;, &gt;, and &amp; by
        their corresponding entity references. 

    <p>
        Implementation note: as a (possibly misguided) performance hack, 
        the obvious cascade of replaceAll calls is replaced by an explicit
        loop that looks for all three special characters at once.
	 */
	public static String substituteEntitiesInElementContent( String s ) 
	{
		Matcher m = elementContentEntities.matcher( s );
		if (!m.find())
			return s;
		else
		{
			int start = 0;
			StringBuffer result = new StringBuffer();
			do
			{
				result.append( s.substring( start, m.start() ) );
				char ch = s.charAt( m.start() );
				switch ( ch )
				{
				case '\r': result.append( "&#xD;" ); break;
				case '<': result.append( "&lt;" ); break;
				case '&': result.append( "&amp;" ); break;
				case '>': result.append( "&gt;" ); break;
				default: throw new CannotEncodeCharacterException( ch, "XML" );
				}
				start = m.end();
			} while (m.find( start ));
			result.append( s.substring( start ) );
			return result.toString();
		}
	}

	public static boolean isNetResource(String resourceAddress) {
		boolean result = false;
		try {
			URL url = new URL(resourceAddress);
			URLConnection conn = url.openConnection();
			conn.connect();
			result = true;			
		} catch(Exception e) {
			
		}
		
		return result;
	}


	
	public static ZExp replaceColumnNames(ZExp exp, Map<String, String> mapOldNewAlias) {
		ZExp result = exp;
		
		for(String oldName : mapOldNewAlias.keySet()) {
			String newName = mapOldNewAlias.get(oldName);
			result = ODEMapsterUtility.replaceColumnNames(result, oldName, newName);
		}
		
		return result;
	}
	
	
	public static ZExp replaceColumnNames(ZExp exp, String oldName, String newName) {
		ZExp result = exp;

		if(exp instanceof ZExpression) {
			ZExpression expression = (ZExpression) exp;
			Vector<ZExp> operands = expression.getOperands();
			ZExpression newExpression = new ZExpression(expression.getOperator());
			for(ZExp operand : operands) {
				ZExp newOperand = ODEMapsterUtility.replaceColumnNames(operand, oldName, newName);
				newExpression.addOperand(newOperand);
			}
			result = newExpression;
		} else if(exp instanceof ZConstant) {
			ZConstant constant = (ZConstant) exp;
			if(constant.getType() == ZConstant.COLUMNNAME) {
				String oldColumnName = constant.getValue();
				if(oldName.equals(oldColumnName)) {
					ZConstant newConstant = new ZConstant(newName, ZConstant.COLUMNNAME);
					result = newConstant;
				}
			}
		} else {
			result = exp;
		}

		return result;
	}
	
	
}

