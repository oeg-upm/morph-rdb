package es.upm.fi.dia.oeg.obdi.core.model;

import java.util.Collection;

import com.hp.hpl.jena.rdf.model.Resource;





public abstract class AbstractPropertyMapping extends AbstractRDB2RDFMapping implements IPropertyMapping{
	public abstract String getMappedPredicateName(int index);
	public abstract Collection<String> getMappedPredicateNames();
	
	protected AbstractConceptMapping parent;

	protected Resource resource;
	
	public AbstractConceptMapping getParent() {
		return parent;
	}
	
	//public abstract MappingType getPropertyMappingType(int index);

//	public boolean isObjectPropertyMapping(int index) {
//		return MappingType.RELATION == this.getPropertyMappingType(index);
//	}
//
//	public boolean isDataPropertyMapping(int index) {
//		return MappingType.ATTRIBUTE == this.getPropertyMappingType(index);
//	}

	public Resource getResource() {
		return resource;
	}

	public void setResource(Resource resource) {
		this.resource = resource;
	}

	
	
}
