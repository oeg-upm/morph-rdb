package es.upm.dia.fi.oeg.morph.r2rml.model

import scala.collection.JavaConversions._
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.engine.R2RMLElement
import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument
import es.upm.fi.dia.oeg.morph.base.ConfigurationProperties
import org.apache.log4j.Logger
import es.upm.fi.dia.oeg.morph.base.DBMetaData
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.util.FileManager
import com.hp.hpl.jena.vocabulary.RDF
import es.upm.fi.dia.oeg.morph.base.Constants
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTriplesMap
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.engine.R2RMLElementVisitor
import es.upm.fi.dia.oeg.obdi.core.model.IAttributeMapping
import es.upm.fi.dia.oeg.obdi.core.model.IRelationMapping
import es.upm.fi.dia.oeg.obdi.core.model.AbstractRDB2RDFMapping.MappingType
import java.util.Collection
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTermMap.TermMapType
import es.upm.fi.dia.oeg.obdi.core.model.AbstractPropertyMapping
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLPredicateObjectMap

class R2RMLMappingDocument(mdPath:String, props:ConfigurationProperties) 
extends AbstractMappingDocument with R2RMLElement {
	val logger = Logger.getLogger(this.getClass().getName());
	this.mappingDocumentPath = mdPath;
	this.configurationProperties = props;
	
	if(configurationProperties != null) {
		val conn = configurationProperties.conn;
		if(conn != null) {
			super.setConn(conn);
			val databaseName = configurationProperties.databaseName;
			val databaseType = configurationProperties.databaseType;
			if(databaseName != null) {
				logger.debug("building metadata.");
				super.setDbMetaData(DBMetaData.buildDBMetaData(conn, databaseName, databaseType));
			}
		}
	}

   this.parse();
	

	def accept(visitor:R2RMLElementVisitor) : Object  = {
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
				val tm = new R2RMLTriplesMap(triplesMapResource, this);
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

	override def getRelationMappings() : java.util.Collection[IRelationMapping] = {
		// TODO Auto-generated method stub
		logger.warn("TODO: Implement getRelationMappings()");
		null;
	}

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
		
		val result:Iterable[AbstractConceptMapping] = if(om != null && rom == null) {
			if(Constants.R2RML_IRI_URI.equals(om.getTermType())) {
				if(cms != null) {
				  Nil
				} else {
					cms.toList.flatMap(cm => {
						val tm = cm.asInstanceOf[R2RMLTriplesMap];
						if(TermMapType.TEMPLATE == om.getTermMapType()) {
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
			val parentTriplesMap = rom.getParentTriplesMap();
			
			val parentSubjectMap = parentTriplesMap.getSubjectMap();
			if(parentSubjectMap.getTermMapType() == TermMapType.TEMPLATE) {
				val templateString = parentSubjectMap.getTemplateString();
				if(cms == null) {
				  Nil
				} else {
					cms.flatMap(cm => {
						if(cm.isPossibleInstance(templateString)) {
							val tm2 = cm.asInstanceOf[R2RMLTriplesMap];
							val classURIs = tm2.getSubjectMap().getClassURIs();
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