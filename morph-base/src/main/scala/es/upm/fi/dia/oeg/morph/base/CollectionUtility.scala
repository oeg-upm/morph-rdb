package es.upm.fi.dia.oeg.morph.base
import scala.collection.JavaConversions._
import java.util.Collection

object CollectionUtility {
	def mkString(theCollection : Collection[Any], sep:String, start:String, end:String) : String = {
	  theCollection.mkString(start, sep, end);
	}

	def getElementsStartWith(theCollection : Collection[String], prefix:String) : Collection[String] = {
	  val result = theCollection.flatMap(collectionElement => {
	    if(collectionElement.startsWith(prefix)) {
	      Some(collectionElement);
	    } else {
	      None;
	    }
	  })
	  
	  result;
	} 

}