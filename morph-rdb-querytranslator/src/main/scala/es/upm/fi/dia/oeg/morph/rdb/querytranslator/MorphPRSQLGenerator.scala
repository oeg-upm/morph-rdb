package es.upm.fi.dia.oeg.morph.rdb.querytranslator

import scala.collection.JavaConversions._

import java.util.Collection;
import java.util.Vector;

import org.apache.log4j.Logger;

import Zql.ZConstant;
import Zql.ZSelectItem;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.vocabulary.RDF;

import es.upm.fi.dia.oeg.morph.base.Constants;
import es.upm.fi.dia.oeg.morph.base.SPARQLUtility;
import es.upm.fi.dia.oeg.morph.querytranslator.NameGenerator;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractPropertyMapping;
import es.upm.fi.dia.oeg.obdi.core.querytranslator.AbstractBetaGenerator;
import es.upm.fi.dia.oeg.obdi.core.querytranslator.AbstractPRSQLGenerator;
import es.upm.fi.dia.oeg.obdi.core.querytranslator.AbstractQueryTranslator;
import es.upm.fi.dia.oeg.obdi.core.querytranslator.AlphaResult;
import es.upm.fi.dia.oeg.obdi.core.querytranslator.QueryTranslationException;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLObjectMap;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLPredicateObjectMap;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLRefObjectMap;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLSubjectMap;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTriplesMap;
import es.upm.fi.dia.oeg.upm.morph.sql.MorphSQLSelectItem;

class MorphPRSQLGenerator(owner: AbstractQueryTranslator) 
extends AbstractPRSQLGenerator(owner: AbstractQueryTranslator) {
	val logger = Logger.getLogger("MorphPRSQLGenerator");

		
	override def genPRSQLObject(tp:Triple ,alphaResult:AlphaResult , betaGenerator:AbstractBetaGenerator 
	    ,nameGenerator:NameGenerator , cmSubject:AbstractConceptMapping ,predicateURI:String 
	    , columnType:String) : Collection[ZSelectItem] = {
		val tpObject = tp.getObject();
		
		val result:List[ZSelectItem] = {
			if(!SPARQLUtility.isBlankNode(tpObject)) {
				if(RDF.`type`.getURI().equalsIgnoreCase(predicateURI)) {
					val tm = cmSubject.asInstanceOf[R2RMLTriplesMap];
					val classURIs = tm.getSubjectMap().getClassURIs();
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
	
	def genPRSQLObjectMappingId(tpObject:Node , cmSubject:AbstractConceptMapping , predicateURI:String ) = {
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
					val propertyMapping = propertyMappings.iterator().next();
					if(propertyMapping.isInstanceOf[R2RMLPredicateObjectMap]) {
						val pom = propertyMapping.asInstanceOf[R2RMLPredicateObjectMap];
						val om = pom.getObjectMap();
						val mappingHashCode = {
							if(om != null) {
								val omHashCode = om.hashCode();
								//this.getOwner().getMapHashCodeMapping().put(mappingHashCode, om);
								this.getOwner().putMappedMapping(omHashCode, om);
								omHashCode;
							} else {
								val rom = pom.getRefObjectMap();
								if(rom != null) {
									//this.getOwner().getMapHashCodeMapping().put(mappingHashCode, rom);
									val romHashCode = rom.hashCode();
									this.getOwner().putMappedMapping(romHashCode, rom);
									romHashCode;
								} else {
								 -1; 
								}
							}					  
						}
	
						
						if(mappingHashCode != -1) {
							val mappingHashCodeConstant = new ZConstant(
									mappingHashCode + "", ZConstant.NUMBER);
							val dbType = this.getOwner().getDatabaseType();
	//						SQLSelectItem mappingSelectItem = new SQLSelectItem();
	//						mappingSelectItem.setExpression(mappingHashCodeConstant);
	//						mappingSelectItem.setDbType(dbType);
	//						mappingSelectItem.setColumnType(Constants.POSTGRESQL_COLUMN_TYPE_INTEGER());
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
	
//	@Override
//	public Collection<ZSelectItem> genPRSQLSTG(List<Triple> tripleBlock,
//			List<BetaResultSet> betaResultSet, NameGenerator nameGenerator, AbstractConceptMapping cm) throws Exception {
//
//		Collection<ZSelectItem> prList = this.genPRSQLSTG(tripleBlock, betaResultSet, nameGenerator, cm);
//		return prList;
//	}

	override def  genPRSQLSubject(tp:Triple, alphaResult:AlphaResult , betaGenerator:AbstractBetaGenerator 
			, nameGenerator:NameGenerator , cmSubject:AbstractConceptMapping) : Collection[ZSelectItem] = {
		
		val tpSubject = tp.getSubject();
		val result:List[ZSelectItem] = {
			if(!SPARQLUtility.isBlankNode(tpSubject)) {
				val parentResult = super.genPRSQLSubject(tp, alphaResult
						, betaGenerator, nameGenerator, cmSubject).toList;
				
				val subject = tp.getSubject();
				val selectItemsMappingId = this.genPRSQLSubjectMappingId(subject, cmSubject);
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
		//ZSelectItem selectItemSubject = betaGenerator.calculateBetaSubject(cmSubject);
	}

	def genPRSQLSubjectMappingId(subject:Node , cmSubject:AbstractConceptMapping ) = {
		val result : List[ZSelectItem] = {
			if(subject.isVariable()) {
				val triplesMap = cmSubject.asInstanceOf[R2RMLTriplesMap];
				val subjectMap = triplesMap.getSubjectMap();
				val mappingHashCodeConstant = new ZConstant(subjectMap.hashCode() + "", ZConstant.NUMBER);
				val databaseType = this.getOwner().getDatabaseType();
				val mappingSelectItem = MorphSQLSelectItem.apply(
						mappingHashCodeConstant, databaseType, Constants.POSTGRESQL_COLUMN_TYPE_INTEGER);
				val mappingSelectItemAlias = Constants.PREFIX_MAPPING_ID + subject.getName();
				mappingSelectItem.setAlias(mappingSelectItemAlias);
				val childResult = List(mappingSelectItem);
				
				//this.getOwner().getMapHashCodeMapping().put(subjectMap.hashCode(), subjectMap);
				this.getOwner().putMappedMapping(subjectMap.hashCode(), subjectMap);
				childResult;
			} else {
			  Nil;
			}	
		}

		result;
	}
}