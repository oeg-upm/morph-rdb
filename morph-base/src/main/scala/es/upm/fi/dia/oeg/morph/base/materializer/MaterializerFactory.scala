package es.upm.fi.dia.oeg.morph.base.materializer

import es.upm.fi.dia.oeg.morph.base.Constants


object MaterializerFactory {
	def create(pRDFLanguage:String , outputFileName:String , jenaMode:String ) : MorphBaseMaterializer =  {
		val model = MorphBaseMaterializer.createJenaModel(jenaMode);
		val rdfLanguage = if(pRDFLanguage == null) {
			Constants.OUTPUT_FORMAT_NTRIPLE;
		} else {
		  pRDFLanguage
		}
		
		
		if(rdfLanguage.equalsIgnoreCase(Constants.OUTPUT_FORMAT_NTRIPLE)) {
			val model = MorphBaseMaterializer.createJenaModel(jenaMode);
			val materializer = new NTripleMaterializer(model);
			materializer.outputFileName = outputFileName;
			materializer
		} else if(rdfLanguage.equalsIgnoreCase(Constants.OUTPUT_FORMAT_RDFXML)) {
			val model = MorphBaseMaterializer.createJenaModel(jenaMode);
			val materializer = new RDFXMLMaterializer(model);
			materializer.outputFileName = outputFileName;
			materializer
		} else {
			val model = MorphBaseMaterializer.createJenaModel(jenaMode);
			val materializer = new NTripleMaterializer(model);
			materializer.outputFileName = outputFileName;
			materializer
		}
	}
}