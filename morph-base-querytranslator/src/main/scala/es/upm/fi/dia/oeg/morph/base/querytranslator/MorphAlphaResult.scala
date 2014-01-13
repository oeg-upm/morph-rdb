package es.upm.fi.dia.oeg.morph.base.querytranslator

import scala.collection.JavaConversions._
import es.upm.fi.dia.oeg.obdi.core.sql.SQLLogicalTable
import es.upm.fi.dia.oeg.obdi.core.sql.SQLJoinTable
import java.util.Collection;
import java.util.List;
import java.util.Vector;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.vocabulary.RDF;

import es.upm.fi.dia.oeg.obdi.core.exception.QueryTranslationException;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractPropertyMapping;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLJoinTable;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLLogicalTable;

class MorphAlphaResult(val alphaSubject:SQLLogicalTable, var alphaPredicateObjects:java.util.List[SQLJoinTable] , val predicateURI:String ) {
}