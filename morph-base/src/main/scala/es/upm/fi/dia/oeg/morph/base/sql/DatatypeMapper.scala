package es.upm.fi.dia.oeg.morph.base.sql
import scala.collection.JavaConversions._
import java.sql.Types
//import com.hp.hpl.jena.datatypes.xsd.XSDDatatype
import org.apache.jena.datatypes.xsd.XSDDatatype;
import java.sql.ResultSetMetaData
import org.slf4j.LoggerFactory


class DatatypeMapper {
  val logger = LoggerFactory.getLogger(this.getClass());

  var mapXMLDatatype = Map[String, String]();
  var mapDBDatatype = Map[String, Integer]();
  val mapDatatype = Map(
    new Integer(Types.BINARY) -> XSDDatatype.XSDhexBinary.getURI()

    , new Integer(Types.NUMERIC) -> XSDDatatype.XSDdecimal.getURI()
    , new Integer(Types.DECIMAL) -> XSDDatatype.XSDdecimal.getURI()

    , new Integer(Types.SMALLINT) -> XSDDatatype.XSDinteger.getURI()
    , new Integer(Types.INTEGER) -> XSDDatatype.XSDinteger.getURI()
    , new Integer(Types.BIGINT) -> XSDDatatype.XSDinteger.getURI()

    , new Integer(Types.FLOAT) -> XSDDatatype.XSDdouble.getURI()
    , new Integer(Types.REAL) -> XSDDatatype.XSDdouble.getURI()
    , new Integer(Types.DOUBLE) -> XSDDatatype.XSDdouble.getURI()

    , new Integer(Types.BOOLEAN) -> XSDDatatype.XSDboolean.getURI()

    , new Integer(Types.DATE) -> XSDDatatype.XSDdate.getURI()
    , new Integer(Types.TIME) -> XSDDatatype.XSDtime.getURI()
    , new Integer(Types.TIMESTAMP) -> XSDDatatype.XSDdateTime.getURI()

    //, new Integer(Types.CHAR) -> XSDDatatype.XSDstring.getURI()
  );

  def getMapXMLDatatype() : Map[String, String] = {
    this.mapXMLDatatype;
  }

  def getMapDBDatatype() : Map[String, Integer] = {
    this.mapDBDatatype;
  }

  def getMappedType(sqlType : Integer) = {
    if(mapDatatype contains sqlType) {
      mapDatatype(sqlType);
    } else {
      null;
    }
  }

  def mapResultSetTypes(rsmd : ResultSetMetaData) = {
    try {
      val columnCount = rsmd.getColumnCount();
      for (i <- 0 until columnCount) {
        val columnName = rsmd.getColumnName(i+1);
        val columnType = rsmd.getColumnType(i+1);

        //logger.info("rsmd.getColumnClassName(i+1) = " + rsmd.getColumnClassName(i+1));
        //logger.info("rsmd.getColumnTypeName(i+1) = " + rsmd.getColumnTypeName(i+1));

        val mappedDatatype = this.getMappedType(columnType);
        if(mappedDatatype != null) {
          mapXMLDatatype += (columnName -> mappedDatatype);
        }

        mapDBDatatype += (columnName -> new Integer(columnType));
      }
    } catch {
      case e:Exception => {
        val errorMessage = "Error mapping data types, error message = " + e.getMessage();
        logger.error(errorMessage);
      }
    }
  }

}