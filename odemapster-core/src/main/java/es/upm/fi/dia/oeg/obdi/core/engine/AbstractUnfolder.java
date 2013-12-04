package es.upm.fi.dia.oeg.obdi.core.engine;

import java.util.Collection;
import java.util.Set;

import Zql.ZUtils;
import es.upm.fi.dia.oeg.morph.base.Constants;
import es.upm.fi.dia.oeg.obdi.core.ILogicalQuery;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument;

public abstract class AbstractUnfolder {
	//protected ConfigurationProperties properties;
	protected String dbType = Constants.DATABASE_MYSQL();
//	protected AbstractRunner owner;
	
	protected AbstractUnfolder() {
		ZUtils.addCustomFunction("concat", 2);
		ZUtils.addCustomFunction("substring", 3);
		ZUtils.addCustomFunction("convert", 2);
		ZUtils.addCustomFunction("coalesce", 2);
		ZUtils.addCustomFunction("abs", 1);
		ZUtils.addCustomFunction("lower", 1);
		ZUtils.addCustomFunction("REPLACE", 3);
		ZUtils.addCustomFunction("TRIM", 1);
//		this.owner = owner;
	}
	
	protected abstract Collection<String> unfold(Set<ILogicalQuery> logicalQueries, AbstractMappingDocument mapping) throws Exception;

	public abstract String unfoldConceptMapping(AbstractConceptMapping mapping) throws Exception;
	
	protected abstract Collection<String> unfold(AbstractMappingDocument mappingDocument) throws Exception;

	public void setDbType(String dbType) {
		this.dbType = dbType;
	}

}
