package es.upm.fi.dia.oeg.morph.base.model

import scala.collection.JavaConversions._
import java.sql.Connection
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import es.upm.fi.dia.oeg.morph.base.sql.MorphDatabaseMetaData
import org.apache.log4j.Logger

abstract class MorphBaseMappingDocument(val classMappings:Iterable[MorphBaseClassMapping]) {
	val logger = Logger.getLogger(this.getClass());
	
	var mappingDocumentPrefixMap:Map[String, String] = Map.empty ;
	var id:String =null;
	var name:String =null;
	var purpose:String =null;
	//var configurationProperties:ConfigurationProperties =null;
	var dbMetaData:MorphDatabaseMetaData  = null;
	var mappingDocumentPath:String = null;
	
	def buildMetaData(connection:Connection, databaseName:String
	    , databaseType:String);
	
	def getMappedConcepts() : java.util.Collection[String] = {
		this.classMappings.map(cm => cm.getConceptName());
	}
	
	def getConceptMappingsByConceptName(conceptURI:String ) = {
	  this.classMappings.filter(
	      cm => cm.getMappedClassURIs.exists(mappedClass => mappedClass.equals(conceptURI))
	      )
	}
	
	def setMappingDocumentPrefixMap(prefixMap:java.util.Map[String, String]) = {
	  this.mappingDocumentPrefixMap = prefixMap.toMap;
	}
	
	def getMappedProperties() : java.util.Collection[String];
	
//	def getConceptMappingByMappingId(conceptMappingId:String) = {
//	  this.classMappings.find(cm => conceptMappingId.equals(cm.id))
//	  
//	}

	def getConceptMappingByPropertyUri(propertyUri:String) : Iterable[MorphBaseClassMapping] = {
	  this.classMappings.filter(cm => {
	    val pms = cm.getPropertyMappings(propertyUri);
	    !pms.isEmpty
	    })
	}

	def getConceptMappingByPropertyURIs(propertyURIs:Iterable[String]) 
	: Iterable[MorphBaseClassMapping]  = {
		this.classMappings.flatMap(cm => {
			val pms = cm.propertyMappings;
			val mappedPredicateNames = pms.map(pm => pm.getMappedPredicateNames).flatten.toSet;
			
			if(propertyURIs.toSet.subsetOf(mappedPredicateNames)) {
			  Some(cm);
			} else {None}
		})
	}
	
//	def getDistinctConceptMappingsNames() : Iterable[String] = {
//		this.classMappings.map(cm => cm.name).toList.distinct
//	}
	
//	def getPropertyMappings() : Iterable[MorphBasePropertyMapping] = {
//	  this.classMappings.map(cm => cm.getPropertyMappings).flatten
//	}
	

	
//	public Collection<AbstractPropertyMapping> getPropertyMappingsByPropertyURI(
//			String propertyURI) {
//		Collection<AbstractPropertyMapping> result = new ArrayList<AbstractPropertyMapping>();
//		for(AbstractConceptMapping conceptmapDef : this.classMappings) {
//			Collection<AbstractPropertyMapping> pms = conceptmapDef.getPropertyMappings();
//			for(AbstractPropertyMapping pm : pms) {
//				if(pm.getName().equals(propertyURI)) {
//					result.add(pm);
//				}
//			}
//		}
//		return result;
//	}
	
	def getPropertyMappingsByPropertyURI(propertyURI:String ) 
	: Iterable[MorphBasePropertyMapping] = {
	  this.classMappings.map(cm => cm.getPropertyMappings(propertyURI)).flatten
	}


	
//	public AbstractPropertyMapping getPropertyMappingByPropertyMappingID(
//			String propertyMappingID) {
//		for(AbstractConceptMapping conceptmapDef : this.classMappings) {
//			Collection<AbstractPropertyMapping> pms = conceptmapDef.getPropertyMappings();
//			for(AbstractPropertyMapping pm : pms) {
//				if(pm.getId().equals(propertyMappingID)) {
//					return pm;
//				}
//			}
//		}
//		return null;
//	}
	
//	protected Collection<IRelationMapping> getRelationMappings() {
//		Collection<IRelationMapping> result = new Vector<IRelationMapping>();
//		Collection<AbstractPropertyMapping> propertyMappings = this.getPropertyMappings();
//		if(propertyMappings != null) {
//			for(AbstractPropertyMapping propertyMapping : propertyMappings) {
//				MappingType propertyMappingType = propertyMapping.getPropertyMappingType();
//				if(propertyMappingType == MappingType.RELATION) {
//					result.add((IRelationMapping) propertyMapping);
//				}
//			}
//		}
//		return result;
//	}
	
//	protected Collection<IRelationMapping> getRelationMappings() {
//		Collection<IRelationMapping> result = new Vector<IRelationMapping>();
//		if(this.classMappings != null) {
//			for(AbstractConceptMapping cm : this.classMappings) {
//				result.addAll(cm.getRelationMappings());
//			}
//		}
//		return result;
//	}
	
//	public Collection<IRelationMapping> getRelationMappingsByRangeClassName(
//			String rangeClassName) {
//		Collection<IRelationMapping> result = new ArrayList<IRelationMapping>();
//		for(AbstractConceptMapping classMapping : this.classMappings) {
//			Collection<IRelationMapping> rms = classMapping.getRelationMappings();
//			for(IRelationMapping rm : rms) {
//				String rangeConceptID = rm.getRangeClassMapping();
//				if(rangeConceptID != null) {
//					AbstractConceptMapping rangeCM = (AbstractConceptMapping) this.getConceptMappingByMappingId(rangeConceptID);
//					if(rangeClassName.equals(rangeCM.getConceptName())) {
//						result.add(rm);
//					}
//					
//				}
//			}
//		}
//		return result;
//	}




	
	def getPossibleRange(predicateURI:String , cm:MorphBaseClassMapping ):Iterable[MorphBaseClassMapping];
	def getPossibleRange(predicateURI:String ):Iterable[MorphBaseClassMapping] ;
	def getPossibleRange(pm:MorphBasePropertyMapping ):Iterable[MorphBaseClassMapping] ;

}