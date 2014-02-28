package es.upm.dia.fi.oeg.morph.r2rml.model

import scala.collection.JavaConversions._
import es.upm.fi.dia.oeg.morph.base.ConfigurationProperties
import org.apache.log4j.Logger
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.util.FileManager
import com.hp.hpl.jena.vocabulary.RDF
import es.upm.fi.dia.oeg.morph.base.Constants
import java.util.Collection
import es.upm.fi.dia.oeg.morph.base.sql.MorphDatabaseMetaData
import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping
import es.upm.fi.dia.oeg.obdi.core.model.AbstractPropertyMapping
import es.upm.fi.dia.oeg.obdi.core.model.IAttributeMapping
import es.upm.fi.dia.oeg.obdi.core.model.IRelationMapping
import es.upm.fi.dia.oeg.obdi.core.model.AbstractRDB2RDFMapping.MappingType
import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument
import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument
import es.upm.dia.fi.oeg.morph.r2rml.MorphR2RMLElement
import es.upm.dia.fi.oeg.morph.r2rml.MorphR2RMLElementVisitor
import java.sql.Connection

class R2RMLMappingDocument(mdPath:String, props:ConfigurationProperties) 
extends AbstractMappingDocument with MorphR2RMLElement {
	val logger = Logger.getLogger(this.getClass().getName());
	super.setMappingDocumentPath(mdPath);
	super.setConfigurationProperties(props);
	
   this.parse();
   
   //BUILDING METADATA
   try {
	   this.buildMetaData(null);
   } catch {
     case e:Exception => {
       logger.warn("Error while building metadata.")
     }
   }
   
   
   def buildMetaData(conn:Connection) = {
     if(this.dbMetaData == null && this.configurationProperties != null && conn != null) {
	     this.dbMetaData = MorphDatabaseMetaData(conn, configurationProperties);
	     this.getConceptMappings().foreach(cm => cm.buildMetaData(this.dbMetaData));       
     }

   }
	

	def accept(visitor:MorphR2RMLElementVisitor) : Object  = {
		val result = visitor.visit(this);
		result;
	}
	
	def parse() = {
		val inputFileName = this.getMappingDocumentPath();

		val model = ModelFactory.createDefaultModel();
		// use the FileManager to find the input file
		val in = FileManager.get().open( inputFileName );
		if (in == null) {
			throw new IllegalArgumentException(
					"Mapping File: " + inputFileName + " not found");
		}
		logger.info("Parsing mapping document " + this.mappingDocumentPath);
		
		// read the Turtle file
		model.read(in, null, "TURTLE");

		super.setMappingDocumentPrefixMap(model.getNsPrefixMap());
		
		val triplesMapResources = model.listResourcesWithProperty(RDF.`type`
				, Constants.R2RML_TRIPLESMAP_CLASS);
		if(triplesMapResources != null) {
			this.classMappings = new java.util.Vector[AbstractConceptMapping]();
			while(triplesMapResources.hasNext()) {
				val triplesMapResource = triplesMapResources.nextResource();
				val triplesMapKey = triplesMapResource.getLocalName();
				val tm = R2RMLTriplesMap(triplesMapResource);
				tm.setId(triplesMapKey);
				this.classMappings.add(tm);
			}

		}
	}
	
	override def getMappingDocumentID() : String = { 
	  // TODO Auto-generated method stub
		null;
	}
	
	override def getMappedProperties() : java.util.List[String] = {
		val cms = this.classMappings.toList;
		val resultAux = cms.map(cm => {
			val tm = cm.asInstanceOf[R2RMLTriplesMap];
			val pms = tm.getPropertyMappings().toList;
			val mappedPredicateNames = pms.map(pm => {pm.getMappedPredicateNames().toList});
			val flatMappedPredicateNames = mappedPredicateNames.flatten;
			flatMappedPredicateNames;
		})
		
		val result = resultAux.flatten;
		result ;
	}

	override def getMappedAttributes() : java.util.List[String] = {
		// TODO Auto-generated method stub
		logger.warn("TODO: Implement getMappedAttributes()");
		null;
	}

	override def getAttributeMappings() : java.util.Collection[IAttributeMapping] = {
		// TODO Auto-generated method stub
		logger.warn("TODO: Implement getAttributeMappings()");
		null;
	}

	override def getAttributeMappings(domain:String ,range:String ) : java.util.Collection[IAttributeMapping] = {
		// TODO Auto-generated method stub
		logger.warn("TODO: Implement getAttributeMappings(String domain,String range)");
		null;
	}

	override def getMappedRelations() : java.util.List[String] = {
		// TODO Auto-generated method stub
		logger.warn("TODO: Implement getMappedRelations()");
		null;
	}

//	override def getRelationMappings() : java.util.Collection[IRelationMapping] = {
//		// TODO Auto-generated method stub
//		logger.warn("TODO: Implement getRelationMappings()");
//		null;
//	}

	override def getRelationMappings(domain:String ,range:String ) 
	: java.util.Collection[IRelationMapping]  = {
		// TODO Auto-generated method stub
		logger.warn("TODO: Implement getRelationMappings(String domain,String range)");
		null;
	}

	override def getMappedConceptURI(conceptMappingID:String ) : String  = {
		// TODO Auto-generated method stub
		logger.warn("TODO: Implement getMappedConceptURI(String conceptMappingID)");
		null;
	}

	override def getPropertyMappingType(propertyMappingID:String ) : MappingType = {
		// TODO Auto-generated method stub
		logger.warn("TODO: Implement getMappingType(String propertyMappingID)");
		null;
	}

	def setTriplesMaps(triplesMaps:Collection[AbstractConceptMapping] ) = {
		this.classMappings = triplesMaps;
	}	

	def getParentTripleMap(pRom:R2RMLRefObjectMap) : R2RMLTriplesMap = {
	  val parentTripleMapResources = this.classMappings.map(cm => {
	    val tm = cm.asInstanceOf[R2RMLTriplesMap];
	    val poms = tm.predicateObjectMaps;
	    poms.map(pom => {
	      val roms = pom.refObjectMaps;
	      roms.flatMap(rom => {
	    	  if(rom == pRom) {
	    	    Some(rom.parentTriplesMapResource);
	    	  } else {None}	        
	      });
	    }).flatten
	  }).flatten;
	  
	  val parentTripleMaps = this.classMappings.filter(cm => {
	    val tm = cm.asInstanceOf[R2RMLTriplesMap];
	    parentTripleMapResources.exists(parentTripleMapResource => {
	      tm.getResource ==parentTripleMapResource;
	    })
	  })

	  val parentTripleMap = parentTripleMaps.iterator.next;
	  parentTripleMap.asInstanceOf[R2RMLTriplesMap]
	}
	
	override def getPossibleRange(predicateURI:String ) : java.util.Set[AbstractConceptMapping] = {
		val pms = this.getPropertyMappingsByPropertyURI(predicateURI).toList;
		val resultAux = if(pms != null ) {
			pms.map(pm => {
				val possibleRange = this.getPossibleRange(pm).toList;
				possibleRange;
			})
		} else {
		  Nil
		}
		
		val resultInList = resultAux.flatten;
		val resultInSet = resultInList.toSet;
		resultInSet
	}


	override def getPossibleRange(predicateURI:String , cm:AbstractConceptMapping ) 
	: java.util.Set[AbstractConceptMapping] = {
		val pms = cm.getPropertyMappings(predicateURI);
		val result = if(pms != null) {
		  pms.toList.map(pm => {
			val possibleRange = this.getPossibleRange(pm);
			possibleRange;		    
		  })
		} else {
		  Nil
		}
		
		val resultInList = result.flatten;
		val resultInSet = resultInList.toSet;
		resultInSet;
	}


	override def  getPossibleRange(pm:AbstractPropertyMapping) 
	: java.util.Set[AbstractConceptMapping] = {
		
		val pom = pm.asInstanceOf[R2RMLPredicateObjectMap];
		val om = pom.getObjectMap(0);
		val rom = pom.getRefObjectMap(0);
		val cms = this.getConceptMappings();
		val inferredTermType = om.inferTermType;
		
		val result:Iterable[AbstractConceptMapping] = if(om != null && rom == null) {
			if(Constants.R2RML_IRI_URI.equals(inferredTermType)) {
				if(cms != null) {
				  Nil
				} else {
					cms.toList.flatMap(cm => {
						val tm = cm.asInstanceOf[R2RMLTriplesMap];
						if(Constants.MorphTermMapType.TemplateTermMap == om.termMapType) {
							val objectTemplateString = om.getTemplateString();
							if(tm.isPossibleInstance(objectTemplateString)) {
								Some(cm);
							} else {
							  None
							}
						} else {
						  None
						}
					})
				}
			} else {
			  Nil
			}
		} else if(rom != null && om == null) {
			//val parentTriplesMap = rom.getParentTriplesMap().asInstanceOf[R2RMLTriplesMap];
			val parentTriplesMap = this.getParentTripleMap(rom);
			
			val parentSubjectMap = parentTriplesMap.subjectMap;
			if(parentSubjectMap.termMapType == Constants.MorphTermMapType.TemplateTermMap) {
				val templateString = parentSubjectMap.getTemplateString();
				if(cms == null) {
				  Nil
				} else {
					cms.flatMap(cm => {
						if(cm.isPossibleInstance(templateString)) {
							val tm2 = cm.asInstanceOf[R2RMLTriplesMap];
							val classURIs = tm2.subjectMap.classURIs;
							if(classURIs != null && !classURIs.isEmpty()) {
								Some(cm);	
							} else {
							  None
							}
						} else {
						  None
						}
					})
				}
			} else {
				List(parentTriplesMap);	
			}
		} else {
		  Nil
		}

		val resultInSet = result.toSet;
		resultInSet
	}
}