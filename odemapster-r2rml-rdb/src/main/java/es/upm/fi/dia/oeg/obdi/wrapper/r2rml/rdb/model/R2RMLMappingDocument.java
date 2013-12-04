package es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model;

import java.io.InputStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.w3c.dom.Element;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.vocabulary.RDF;

import es.upm.fi.dia.oeg.morph.base.ColumnMetaDataFactory;
import es.upm.fi.dia.oeg.morph.base.Constants;
import es.upm.fi.dia.oeg.morph.base.TableMetaData;
import es.upm.fi.dia.oeg.obdi.core.ConfigurationProperties;
import es.upm.fi.dia.oeg.obdi.core.exception.ParseException;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractPropertyMapping;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractRDB2RDFMapping.MappingType;
import es.upm.fi.dia.oeg.obdi.core.model.IAttributeMapping;
import es.upm.fi.dia.oeg.obdi.core.model.IRelationMapping;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.engine.R2RMLElement;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.engine.R2RMLElementVisitor;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.exception.R2RMLInvalidRefObjectMapException;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.exception.R2RMLInvalidTermMapException;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.exception.R2RMLInvalidTriplesMapException;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.exception.R2RMLJoinConditionException;
import es.upm.fi.dia.oeg.obdi.wrapper.r2rml.rdb.model.R2RMLTermMap.TermMapType;

public class R2RMLMappingDocument extends AbstractMappingDocument implements R2RMLElement{
	private static Logger logger = Logger.getLogger(R2RMLMappingDocument.class);
	
	public R2RMLMappingDocument(String mappingDocumentPath
			, ConfigurationProperties configurationProperties) 
					throws Exception {
		super();
		this.mappingDocumentPath = mappingDocumentPath;
		super.configurationProperties = configurationProperties;

		if(configurationProperties != null) {
			Connection conn = configurationProperties.getConn();
			if(conn != null) {
				super.setConn(conn);
				String databaseName = 
						configurationProperties.getDatabaseName();
				String databaseType = 
						configurationProperties.getDatabaseType();
				if(databaseName != null) {
					logger.debug("building metadata.");
					super.tablesMetaData = TableMetaData.buildTablesMetaData(conn, databaseName, databaseType);
					//super.columnsMetaData = ColumnMetaData.buildColumnsMetaData(conn, databaseName, databaseType);
					super.columnsMetaData = ColumnMetaDataFactory.buildColumnsMetaData(conn, databaseName, databaseType);
				}
			}
		}


		Model model = ModelFactory.createDefaultModel();
		// use the FileManager to find the input file
		InputStream in = FileManager.get().open( this.mappingDocumentPath );
		if (in == null) {
			throw new IllegalArgumentException(
					"Mapping File not found: " + this.mappingDocumentPath);
		}
		logger.info("Parsing mapping document " + this.mappingDocumentPath);

		// read the Turtle file
		model.read(in, null, "TURTLE");
		super.setMappingDocumentPrefixMap(model.getNsPrefixMap());

		ResIterator triplesMapResources = model.listResourcesWithProperty(
				RDF.type, Constants.R2RML_TRIPLESMAP_CLASS());
		if(triplesMapResources != null) {
			this.classMappings = new Vector<AbstractConceptMapping>();
			while(triplesMapResources.hasNext()) {
				Resource triplesMapResource = triplesMapResources.nextResource();
				//String triplesMapKey = triplesMapResource.getNameSpace() + triplesMapResource.getLocalName();
				String triplesMapKey = triplesMapResource.getLocalName();
				try {
					R2RMLTriplesMap tm = new R2RMLTriplesMap(triplesMapResource, this);
					tm.setId(triplesMapKey);
					this.classMappings.add(tm);					
				} catch(Exception e) {
					logger.error("Error occured during parsing TriplesMap " + triplesMapResource.getLocalName() + " because " + e.getMessage());
					throw e;
				}

			}
		}
	}


	public Object accept(R2RMLElementVisitor visitor) throws Exception {
		Object result = visitor.visit(this);
		return result;
	}

	public void parse() throws R2RMLInvalidTriplesMapException, R2RMLInvalidRefObjectMapException, R2RMLJoinConditionException, R2RMLInvalidTermMapException {
		String inputFileName = this.getMappingDocumentPath();

		Model model = ModelFactory.createDefaultModel();
		// use the FileManager to find the input file
		InputStream in = FileManager.get().open( inputFileName );
		if (in == null) {
			throw new IllegalArgumentException(
					"File: " + inputFileName + " not found");
		}

		// read the Turtle file
		model.read(in, null, "TURTLE");

		ResIterator triplesMapResources = model.listResourcesWithProperty(RDF.type
				, Constants.R2RML_TRIPLESMAP_CLASS());
		if(triplesMapResources != null) {
			this.classMappings = new Vector<AbstractConceptMapping>();
			while(triplesMapResources.hasNext()) {
				Resource triplesMapResource = triplesMapResources.nextResource();
				R2RMLTriplesMap tm = new R2RMLTriplesMap(triplesMapResource, this);
				this.classMappings.add(tm);
			}

		}


		// write it to standard out
		//model.write(System.out);		
	}


	@Override
	public void parse(Element xmlElement) throws ParseException {
		// TODO Auto-generated method stub

	}

	@Override
	public String getMappingDocumentID() {
		// TODO Auto-generated method stub
		return null;
	}








	@Override
	public List<String> getMappedProperties() {
		List<String> result = new ArrayList<String>();

		Collection<AbstractConceptMapping> cms = this.classMappings;
		for(AbstractConceptMapping cm : cms) {
			R2RMLTriplesMap tm = (R2RMLTriplesMap) cm;
			Collection<AbstractPropertyMapping> pms = tm.getPropertyMappings();
			for(AbstractPropertyMapping pm : pms) {
				result.add(pm.getMappedPredicateName());
			}
		}
		return result ;
	}









	@Override
	public List<String> getMappedAttributes() {
		// TODO Auto-generated method stub
		logger.warn("TODO: Implement getMappedAttributes()");
		return null;
	}

	@Override
	public Collection<IAttributeMapping> getAttributeMappings() {
		// TODO Auto-generated method stub
		logger.warn("TODO: Implement getAttributeMappings()");
		return null;
	}

	@Override
	public Collection<IAttributeMapping> getAttributeMappings(String domain,String range) {
		// TODO Auto-generated method stub
		logger.warn("TODO: Implement getAttributeMappings(String domain,String range)");
		return null;
	}

	@Override
	public List<String> getMappedRelations() {
		// TODO Auto-generated method stub
		logger.warn("TODO: Implement getMappedRelations()");
		return null;
	}

	@Override
	public Collection<IRelationMapping> getRelationMappings() {
		// TODO Auto-generated method stub
		logger.warn("TODO: Implement getRelationMappings()");
		return null;
	}

	@Override
	public Collection<IRelationMapping> getRelationMappings(String domain,String range) {
		// TODO Auto-generated method stub
		logger.warn("TODO: Implement getRelationMappings(String domain,String range)");
		return null;
	}

	@Override
	public String getMappedConceptURI(String conceptMappingID) {
		// TODO Auto-generated method stub
		logger.warn("TODO: Implement getMappedConceptURI(String conceptMappingID)");
		return null;
	}



	@Override
	public MappingType getPropertyMappingType(String propertyMappingID) {
		// TODO Auto-generated method stub
		logger.warn("TODO: Implement getMappingType(String propertyMappingID)");
		return null;
	}

	public void setTriplesMaps(Collection<AbstractConceptMapping> triplesMaps) {
		this.classMappings = triplesMaps;
	}

	public Map<Node, Set<AbstractConceptMapping>> inferByObject(
			AbstractConceptMapping cm, String predicateURI, Node object) {
		Map<Node, Set<AbstractConceptMapping>> result = new HashMap<Node, Set<AbstractConceptMapping>>();

		if(object.isVariable()) {
			Collection<AbstractPropertyMapping> apms = cm.getPropertyMappings(predicateURI);
			for(AbstractPropertyMapping apm : apms) {
				if(apm instanceof R2RMLPredicateObjectMap) {
					R2RMLPredicateObjectMap pom = (R2RMLPredicateObjectMap) apm;
					R2RMLObjectMap om = pom.getObjectMap();
					if(om.getTermMapType() == TermMapType.TEMPLATE) {
						om.getTemplateString();
					}
				}
			}
		}

		return result;
	}

	@Override
	public Set<AbstractConceptMapping> getPossibleRange(String predicateURI) {
		Set<AbstractConceptMapping> result = new HashSet<AbstractConceptMapping>();
		Collection<AbstractPropertyMapping> pms = this.getPropertyMappingsByPropertyURI(predicateURI); 
		for(AbstractPropertyMapping pm : pms) {
			Set<AbstractConceptMapping> possibleRange = this.getPossibleRange(pm);
			result.addAll(possibleRange);
		}
		return result;
	}


	@Override
	public Set<AbstractConceptMapping> getPossibleRange(String predicateURI,
			AbstractConceptMapping cm) {
		Set<AbstractConceptMapping> result = new HashSet<AbstractConceptMapping>();
		Collection<AbstractPropertyMapping> pms = cm.getPropertyMappings(predicateURI);
		for(AbstractPropertyMapping pm : pms) {
			Set<AbstractConceptMapping> possibleRange = this.getPossibleRange(pm);
			result.addAll(possibleRange);
		}
		return result;
	}


	@Override
	public Set<AbstractConceptMapping> getPossibleRange(
			AbstractPropertyMapping pm) {
		Set<AbstractConceptMapping> result = new HashSet<AbstractConceptMapping>();
		
		R2RMLPredicateObjectMap pom = (R2RMLPredicateObjectMap) pm;
		R2RMLObjectMap om = pom.getObjectMap();
		R2RMLRefObjectMap rom = pom.getRefObjectMap();
		
		if(om != null && rom == null) {
			if(Constants.R2RML_IRI_URI().equals(om.getTermType())) {
				for(AbstractConceptMapping cm : this.getConceptMappings()) {
					R2RMLTriplesMap tm = (R2RMLTriplesMap) cm;
					tm.getSubjectMap();
					if(TermMapType.TEMPLATE == om.getTermMapType()) {
						String objectTemplateString = om.getTemplateString();
						if(tm.isPossibleInstance(objectTemplateString)) {
							result.add(cm);
						}
					}
				}
			}
		} else if(rom != null && om == null) {
			R2RMLTriplesMap parentTriplesMap = (R2RMLTriplesMap) rom.getParentTriplesMap();
			R2RMLTermMap parentSubjectMap = parentTriplesMap.getSubjectMap();
			if(parentSubjectMap.getTermMapType() == TermMapType.TEMPLATE) {
				String templateString = parentSubjectMap.getTemplateString();
				for(AbstractConceptMapping cm : this.getConceptMappings()) {
					if(cm.isPossibleInstance(templateString)) {
						R2RMLTriplesMap tm2 = (R2RMLTriplesMap) cm;
						Collection<String> classURIs = tm2.getSubjectMap().getClassURIs();
						if(classURIs != null && !classURIs.isEmpty()) {
							result.add(cm);	
						}
					}
				}				
			} else {
				result.add(parentTriplesMap);	
			}
			

		}

		return result;
	}



}
