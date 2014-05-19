package es.upm.fi.dia.oeg.morph.rdb.querytranslator

import scala.collection.JavaConversions._
import java.util.Collection
import org.apache.log4j.Logger
import Zql.ZConstant
import Zql.ZSelectItem
import com.hp.hpl.jena.graph.Node
import com.hp.hpl.jena.graph.Triple
import com.hp.hpl.jena.vocabulary.RDF
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.morph.base.SPARQLUtility
import es.upm.fi.dia.oeg.morph.base.sql.MorphSQLSelectItem
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLPredicateObjectMap
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLMappingDocument
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBasePRSQLGenerator
import es.upm.fi.dia.oeg.morph.base.querytranslator.NameGenerator
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphAlphaResult
import es.upm.fi.dia.oeg.morph.base.querytranslator.MorphBaseBetaGenerator
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.engine.MorphBaseUnfolder
import es.upm.fi.dia.oeg.morph.r2rml.model.R2RMLTermMap
import es.upm.fi.dia.oeg.morph.r2rml.rdb.engine.MorphRDBUnfolder

class MorphRDBPRSQLGenerator(md:R2RMLMappingDocument, unfolder:MorphRDBUnfolder)
extends MorphBasePRSQLGenerator(md:MorphBaseMappingDocument, unfolder:MorphBaseUnfolder) {
	override val logger = Logger.getLogger("MorphPRSQLGenerator");

		
	override def genPRSQLObject(tp:Triple ,alphaResult:MorphAlphaResult 
	    , betaGenerator:MorphBaseBetaGenerator,nameGenerator:NameGenerator 
	    , cmSubject:MorphBaseClassMapping ,predicateURI:String, columnType:String) 
	: List[ZSelectItem] = {
		val tpObject = tp.getObject();
		
		val result:List[ZSelectItem] = {
			if(!SPARQLUtility.isBlankNode(tpObject)) {
				if(RDF.`type`.getURI().equalsIgnoreCase(predicateURI)) {
					val tm = cmSubject.asInstanceOf[R2RMLTriplesMap];
					val classURIs = tm.subjectMap.classURIs;
					val resultAux = classURIs.map(classURI => {
						val zConstant = new ZConstant(classURI, ZConstant.STRING);
						val selectItem:ZSelectItem = new ZSelectItem();
						selectItem.setExpression(zConstant);
						val selectItemAlias = nameGenerator.generateName(tpObject);
						selectItem.setAlias(selectItemAlias);
						selectItem;					  
					})
					resultAux.toList;
				} else {
					val parentResult = super.genPRSQLObject(tp, alphaResult, betaGenerator
							, nameGenerator, cmSubject, predicateURI, columnType).toList;
					
					val childResult = this.genPRSQLObjectMappingId(tpObject, cmSubject, predicateURI);
					
					if(childResult == null) {
						parentResult
					} else {
					  parentResult ::: childResult
					}
				}
			} else {
			  Nil;
			}
		}

		result;
	}
	
	def genPRSQLObjectMappingId(tpObject:Node , cmSubject:MorphBaseClassMapping 
	    , predicateURI:String ) = {
		val childResult:List[ZSelectItem] = {
			if(tpObject.isVariable() && !SPARQLUtility.isBlankNode(tpObject)) {
				val propertyMappings = 
						cmSubject.getPropertyMappings(predicateURI);
				if(propertyMappings == null || propertyMappings.isEmpty()) {
					logger.warn("no property mappings defined for predicate: " + predicateURI);
					Nil;
				} else if (propertyMappings.size() > 1) {
					logger.warn("multiple property mappings defined for predicate: " + predicateURI);
					Nil;
				} else {
					val propertyMapping = propertyMappings.iterator.next();
					if(propertyMapping.isInstanceOf[R2RMLPredicateObjectMap]) {
						val pom = propertyMapping.asInstanceOf[R2RMLPredicateObjectMap];
						val om = pom.getObjectMap(0);
						val mappingHashCode = {
							if(om != null) {
								val omHashCode = om.hashCode();
								this.putMappedMapping(omHashCode, om);
								omHashCode;
							} else {
								val rom = pom.getRefObjectMap(0);
								if(rom != null) {
									//this.getOwner().getMapHashCodeMapping().put(mappingHashCode, rom);
									val romHashCode = rom.hashCode();
									this.putMappedMapping(romHashCode, rom);
									romHashCode;
								} else {
								 -1; 
								}
							}					  
						}
	
						
						if(mappingHashCode != -1) {
							val mappingHashCodeConstant = new ZConstant(
									mappingHashCode + "", ZConstant.NUMBER);
							val mappingSelectItem = MorphSQLSelectItem.apply(
									mappingHashCodeConstant, dbType, Constants.POSTGRESQL_COLUMN_TYPE_INTEGER);
							val mappingSelectItemAlias = Constants.PREFIX_MAPPING_ID + tpObject.getName();
							mappingSelectItem.setAlias(mappingSelectItemAlias);
	
							List(mappingSelectItem);
						} else {
						  Nil;
						}
					} else {
					  Nil
					}				
				}
	
			} else {
			  Nil;
			}
		  
		}
		
		
		childResult;
	}
	
	override def  genPRSQLSubject(tp:Triple, alphaResult:MorphAlphaResult 
	    , betaGenerator:MorphBaseBetaGenerator, nameGenerator:NameGenerator 
	    , cmSubject:MorphBaseClassMapping) : List[ZSelectItem] = {
		val triplesMap = cmSubject.asInstanceOf[R2RMLTriplesMap];
		val subjectMap = triplesMap.subjectMap;
		
		val tpSubject = tp.getSubject();
		val result:List[ZSelectItem] = {
			if(!SPARQLUtility.isBlankNode(tpSubject)) {
				val parentResult = super.genPRSQLSubject(tp, alphaResult
						, betaGenerator, nameGenerator, cmSubject).toList;
				
				val subject = tp.getSubject();
				val selectItemsMappingId = this.genPRSQLMappingId(subject, subjectMap);
				if(selectItemsMappingId == null) {
					parentResult
				} else {
				  parentResult ::: selectItemsMappingId;
				}			
			} else {
			  Nil;
			}
		}

		result;
	}

	def genPRSQLMappingId(node:Node , termMap:R2RMLTermMap):List[ZSelectItem] = {
		val result : List[ZSelectItem] = {
			if(node.isVariable()) {
				val termMapHashCode = termMap.hashCode();
				val mappingHashCodeConstant = new ZConstant(
				    termMapHashCode + "", ZConstant.NUMBER);
				val mappingSelectItem = MorphSQLSelectItem.apply(
						mappingHashCodeConstant, dbType, Constants.POSTGRESQL_COLUMN_TYPE_INTEGER);
				val mappingSelectItemAlias = Constants.PREFIX_MAPPING_ID + node.getName();
				mappingSelectItem.setAlias(mappingSelectItemAlias);
				val childResult = List(mappingSelectItem);
				
				this.putMappedMapping(termMapHashCode, termMap);
				childResult;
			} else { Nil; }	
		}

		result;
	}
	
	override def genPRSQLPredicateMappingId(node:Node,cm:MorphBaseClassMapping, predicateURI:String ):List[ZSelectItem] = {
//		val triplesMap = cm.asInstanceOf[R2RMLTriplesMap];
//		val poMap = triplesMap.predicateObjectMaps.iterator.next;
		val pms = cm.getPropertyMappings(predicateURI);
		val poMap = pms.iterator.next.asInstanceOf[R2RMLPredicateObjectMap];
		val predicateMap = poMap.predicateMaps.iterator.next;
		val selectItemPredicateMappingId = this.genPRSQLMappingId(node, predicateMap);
		selectItemPredicateMappingId
	}
	
}