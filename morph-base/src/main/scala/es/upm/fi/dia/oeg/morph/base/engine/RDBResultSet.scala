package es.upm.fi.dia.oeg.morph.base.engine

import java.sql.ResultSet

class RDBResultSet(rs:ResultSet ) extends MorphBaseResultSet{
	def next() : Boolean = {
		try { this.rs.next(); } 
		catch  {
		  case e:Exception => { false; }
		}
	}

	def getObject(columnIndex:Int) : Object = {
	  rs.getObject(columnIndex);
	}

	def getObject(columnLabel:String ) : Object = {
	  rs.getObject(columnLabel);
	}

	def getString(columnIndex:Int ) : String  = {
		rs.getString(columnIndex);
	}

	def getString(columnLabel:String ) : String  = {
		rs.getString(columnLabel);
	}

	def getInt(columnIndex:Int ) : Integer = {
		return rs.getInt(columnIndex);
	}

	def getInt(columnLabel:String ) : Integer  = {
		rs.getInt(columnLabel);
	}
}