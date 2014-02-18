package es.upm.fi.dia.oeg.obdi.core.engine;

import java.util.Collection;

import Zql.ZUtils;
import es.upm.fi.dia.oeg.morph.base.Constants;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractConceptMapping;
import es.upm.fi.dia.oeg.obdi.core.model.AbstractMappingDocument;
import es.upm.fi.dia.oeg.obdi.core.sql.SQLQuery;

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
	

	public abstract SQLQuery unfoldConceptMapping(AbstractConceptMapping cm) throws Exception;
	
	public abstract SQLQuery unfoldConceptMapping(AbstractConceptMapping cm, String subjectURI) throws Exception;
	
	public abstract SQLQuery unfoldSubject(AbstractConceptMapping cm) throws Exception;
	
	protected abstract Collection<SQLQuery> unfoldMappingDocument(AbstractMappingDocument mappingDocument);

	public void setDbType(String dbType) {
		this.dbType = dbType;
	}

}
