package es.upm.fi.dia.oeg.morph.base.sql

import es.upm.fi.dia.oeg.morph.base.Constants

class MorphInformationSchema (val tableNameColumn:String, val tableRowsColumn:String
    , val columnNameColumn:String, val datatypeColumn:String, val isNullableColumn:String
    , val characterMaximumLengthColumn:String, val columnKeyColumn:Option[String], val relNameColumn:String) {

}

object MorphInformationSchema {
  def apply(databaseType:String) : MorphInformationSchema = {
    val result:MorphInformationSchema = {
		  if(databaseType.equalsIgnoreCase(Constants.DATABASE_MYSQL)) {
			val tableNameColumn = "TABLE_NAME";
			val tableRowsColumn = "TABLE_ROWS";
			val columnNameColumn = "COLUMN_NAME";
			val datatypeColumn = "DATA_TYPE";
			val isNullableColumn = "IS_NULLABLE";
			val characterMaximumLengthColumn = "CHARACTER_MAXIMUM_LENGTH";
			val columnKeyColumn = Some("COLUMN_KEY");
			val relNameColumn = "TABLE_NAME";
			new MorphInformationSchema(tableNameColumn, tableRowsColumn, columnNameColumn
			    , datatypeColumn, isNullableColumn, characterMaximumLengthColumn, columnKeyColumn, relNameColumn);
		} else if(databaseType.equalsIgnoreCase(Constants.DATABASE_POSTGRESQL)) {
			val tableNameColumn = "table_name";
			val tableRowsColumn = "seq_tup_read";					
			val columnNameColumn = "column_name";
			val datatypeColumn = "data_type";
			val isNullableColumn = "is_nullable";
			val characterMaximumLengthColumn = "CHARACTER_MAXIMUM_LENGTH";
			val columnKeyColumn = None;
			val relNameColumn = "relname";
			new MorphInformationSchema(tableNameColumn, tableRowsColumn, columnNameColumn
			    , datatypeColumn, isNullableColumn, characterMaximumLengthColumn, columnKeyColumn, relNameColumn);
		} else {
			val tableNameColumn = "TABLE_NAME";
			val tableRowsColumn = "TABLE_ROWS";
			val columnNameColumn = "COLUMN_NAME";
			val datatypeColumn = "DATA_TYPE";
			val isNullableColumn = "IS_NULLABLE";
			val characterMaximumLengthColumn = "CHARACTER_MAXIMUM_LENGTH";
			val columnKeyColumn = Some("COLUMN_KEY");
			val relNameColumn = "TABLE_NAME";
			new MorphInformationSchema(tableNameColumn, tableRowsColumn, columnNameColumn
			    , datatypeColumn, isNullableColumn, characterMaximumLengthColumn, columnKeyColumn, relNameColumn);
		}
    }
    result;
  }
}