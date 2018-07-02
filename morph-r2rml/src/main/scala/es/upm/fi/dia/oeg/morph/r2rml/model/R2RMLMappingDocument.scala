package es.upm.fi.dia.oeg.morph.r2rml.model

import scala.collection.JavaConversions._
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.util.FileManager
import org.apache.jena.vocabulary.RDF
import es.upm.fi.dia.oeg.morph.base.Constants
import java.util.Collection
import es.upm.fi.dia.oeg.morph.base.sql.MorphDatabaseMetaData
import es.upm.fi.dia.oeg.morph.r2rml.MorphR2RMLElement
import es.upm.fi.dia.oeg.morph.r2rml.MorphR2RMLElementVisitor
import java.sql.Connection
import es.upm.fi.dia.oeg.morph.base.model.MorphBasePropertyMapping
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseMappingDocument
import es.upm.fi.dia.oeg.morph.base.model.MorphBaseClassMapping
import es.upm.fi.dia.oeg.morph.base.MorphProperties
import org.apache.jena.rdf.model.Model
import java.util.Properties
import es.upm.fi.dia.oeg.morph.base.GitHubUtility
import org.slf4j.LoggerFactory

class R2RMLMappingDocument(classMappings:Iterable[R2RMLTriplesMap]) 
extends MorphBaseMappingDocument(classMappings) with MorphR2RMLElement {
	override   val logger = LoggerFactory.getLogger(this.getClass());

   
	def buildMetaData(conn:Connection, databaseName:String
       , databaseType:String) = {
		if(conn != null && this.dbMetaData == None) {
			val newMetaData = MorphDatabaseMetaData(conn, databaseName, databaseType);
			this.dbMetaData = Some(newMetaData);
			this.classMappings.foreach(cm => cm.buildMetaData(this.dbMetaData));       
		}
	}

	def accept(visitor:MorphR2RMLElementVisitor) : Object  = { visitor.visit(this);  }
	
	override def getMappedProperties() : Iterable[String] = {
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


//	def setTriplesMaps(triplesMaps:Collection[MorphBaseClassMapping] ) = {
//		this.classMappings = triplesMaps.toSet;
//	}	

	def getParentTriplesMap(refObjectMap:R2RMLRefObjectMap) : R2RMLTriplesMap = {
	  val parentTripleMapResources = this.classMappings.map(cm => {
	    val tm = cm.asInstanceOf[R2RMLTriplesMap];
	    val poms = tm.predicateObjectMaps;
	    poms.map(pom => {
	      val roms = pom.refObjectMaps;
	      roms.flatMap(rom => {
	    	  if(rom == refObjectMap) {
	    	    Some(rom.parentTriplesMapResource);
	    	  } else {None}	        
	      });
	    }).flatten
	  }).flatten;
	  
	  val parentTripleMaps = this.classMappings.filter(cm => {
	    val tm = cm.asInstanceOf[R2RMLTriplesMap];
	    parentTripleMapResources.exists(parentTripleMapResource => {
	      tm.resource ==parentTripleMapResource;
	    })
	  })

	  val parentTripleMap = parentTripleMaps.iterator.next;
	  //parentTripleMap.asInstanceOf[R2RMLTriplesMap]
	  
	  //contribution from Frank Michel
	  if (parentTripleMaps.iterator.hasNext) {
		  val parentTripleMap = parentTripleMaps.iterator.next;
		  parentTripleMap.asInstanceOf[R2RMLTriplesMap]
	  } 
	  else {
		  throw new Exception("Error: referenced parent triples map des not exist: " + parentTripleMapResources)
	  }
	  
	}
	
	override def getPossibleRange(predicateURI:String ) : Iterable[MorphBaseClassMapping] = {
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


	override def getPossibleRange(predicateURI:String , cm:MorphBaseClassMapping ) 
	: Iterable[MorphBaseClassMapping] = {
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


	def getObjectMappingPossibleRange(om:R2RMLObjectMap, cms:Iterable[R2RMLTriplesMap])
  : Iterable[MorphBaseClassMapping] = {
    val inferredTermType = om.inferTermType;
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
	}

  def getRefObjectMappingPossibleRange(rom:R2RMLRefObjectMap, cms:Iterable[R2RMLTriplesMap])
  : Iterable[MorphBaseClassMapping] = {
    //val parentTriplesMap = rom.getParentTriplesMap().asInstanceOf[R2RMLTriplesMap];
    val parentTriplesMap = this.getParentTriplesMap(rom);

    val parentSubjectMap = parentTriplesMap.subjectMap;
    
    val result = if(parentSubjectMap.termMapType == Constants.MorphTermMapType.TemplateTermMap) {
      val templateString = parentSubjectMap.getTemplateString();
      val templateStringWithoutColumns = templateString.replaceAll("\\{.*?}", "");
      
      if(cms == null) {
        Nil
      } else {
        cms.flatMap(cm => {
          if(cm.isPossibleInstance(templateString)) {
            val possibleTM = cm.asInstanceOf[R2RMLTriplesMap];
            val possibleSM = possibleTM.subjectMap;
            val possibleSMTemplateString = possibleSM.templateString;
            val possibleSMTemplateStringWithoutColumns = possibleSMTemplateString.replaceAll("\\{.*?}", "");
            
            val classURIs = possibleSM.classURIs;
            if(classURIs != null && !classURIs.isEmpty) {
              if(templateString != null && possibleSMTemplateString != null) {
                if(templateStringWithoutColumns.equals(possibleSMTemplateStringWithoutColumns)) {
                  Some(cm);  
                } else {
                  None
                }
              } else {
                Some(cm);
              }
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
    
    //val result = List(parentTriplesMap);
    result;
  }
	override def getPossibleRange(pm:MorphBasePropertyMapping)
	: Iterable[MorphBaseClassMapping] = {
		
		val pom = pm.asInstanceOf[R2RMLPredicateObjectMap];
		val om:R2RMLObjectMap = pom.getObjectMap(0);
		val rom:R2RMLRefObjectMap = pom.getRefObjectMap(0);
		val cms:Iterable[R2RMLTriplesMap] = this.classMappings

		val result:Iterable[MorphBaseClassMapping] = if(om != null && rom == null) {
      this.getObjectMappingPossibleRange(om, cms);
		} else if(rom != null && om == null) {
      this.getRefObjectMappingPossibleRange(rom, cms);
		} else {
		  Nil
		}

		val resultInSet = result.toSet;
		resultInSet
	}
	
	def getClassMappingsByInstanceTemplate(templateValue:String) 
	: Iterable[MorphBaseClassMapping] = {
	  this.classMappings.filter(cm => {
	    val tm = cm.asInstanceOf[R2RMLTriplesMap]
	    tm.subjectMap.templateString.startsWith(templateValue)
	  })
	}

	def getClassMappingsByInstanceURI(instanceURI:String):Iterable[MorphBaseClassMapping] = {
	  this.classMappings.filter(cm => {
	    val possibleInstance = cm.isPossibleInstance(instanceURI)
	    possibleInstance
	  })
	}

	
}

object R2RMLMappingDocument {
  val logger = LoggerFactory.getLogger(this.getClass());
	
	def apply(mdPath:String)
	: R2RMLMappingDocument = {
	  R2RMLMappingDocument(mdPath, null, null);
	}
	
	def readFromURL(url:String) : Model = {
		val model = ModelFactory.createDefaultModel();
		// use the FileManager to find the input file
		val in = FileManager.get().open(url);
		if (in == null) {
			throw new IllegalArgumentException(
					"Mapping File: " + url + " not found");
		}

		logger.info("Parsing mapping document " + url);
		// read the Turtle file
		model.read(in, null, "TURTLE");
		model;
	}
	
	def readFromGitHubBlob(blobURL:String) : Model = {
	  val rawURL = GitHubUtility.getRawURLFromBlobURL(blobURL);
	  this.readFromURL(rawURL);
	}
	
	def readFromLocalFile(localPath:String) : Model = {
	  this.readFromURL(localPath);
	}
	
	def readFromLocation(location:String) : Model = {
	  if(location.contains("https://github.com") && location.contains("/blob/")) {
	    this.readFromGitHubBlob(location); 
	  } else {
	    this.readFromLocalFile(location);
	  }
	}
	
	def apply(mdPath:String, props:Properties
	    , connection:Connection)
	: R2RMLMappingDocument = {
	  if(mdPath == null) {
			throw new IllegalArgumentException(
					"Mapping File is not defined!");
	  }
	
	  
	  val model = this.readFromLocation(mdPath);
	  
		//contribution from Frank Michel, inferring triples map
		inferTriplesMaps(model);
		 
		val triplesMapResources = model.listResourcesWithProperty(RDF.`type`
				, Constants.R2RML_TRIPLESMAP_CLASS);
		val classMappings = if(triplesMapResources != null) {
		  triplesMapResources.map(triplesMapResource => {
				val triplesMapKey = triplesMapResource.getLocalName();
				val tm = R2RMLTriplesMap(triplesMapResource);
				tm.id = triplesMapKey;
				tm;		    
		  })
		} else {
		  Set.empty
		}
		
		val md = new R2RMLMappingDocument(classMappings.toSet);
		md.mappingDocumentPath = mdPath;
	
		if(connection != null) {
		  //BUILDING METADATA
		  
		   try {
		     val morphProperties = props.asInstanceOf[MorphProperties];
			   md.buildMetaData(connection, morphProperties.databaseName, morphProperties.databaseType );
		   } catch {
		     case e:Exception => { logger.warn("Error occured while building metadata.") }
		   }
		}
   
		//md.configurationProperties = props;
		md.mappingDocumentPrefixMap = model.getNsPrefixMap().toMap;
		md
		
	}
	
	/**
	 *  Add triples with rdf:type rr:TriplesMap for resources that have one rr:logicalTable
	 *  Contribution from Frank Michel
	 */
	private def inferTriplesMaps(model: Model) {
		val stmtsLogTab = model.listStatements(null, Constants.R2RML_LOGICALTABLE_PROPERTY, null);
		for (stmt <- stmtsLogTab) {
			val stmtType = model.createStatement(stmt.getSubject, RDF.`type`, Constants.R2RML_TRIPLESMAP_CLASS);
			model.add(stmtType)
		}
	}

}