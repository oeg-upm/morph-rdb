package es.upm.fi.dia.oeg.morph.base.materializer

import es.upm.fi.dia.oeg.morph.base.Constants
import java.io.OutputStream
import java.io.FileOutputStream
import java.io.Writer


object MaterializerFactory {
  def create(pRDFLanguage:String , writer:Writer, jenaMode:String
             , outputStream: OutputStream )	: MorphBaseMaterializer =  {
    val model = MorphBaseMaterializer.createJenaModel(jenaMode);
    val rdfLanguage = if(pRDFLanguage == null) { Constants.OUTPUT_FORMAT_NTRIPLE; }
    else { pRDFLanguage }

    if(rdfLanguage.equalsIgnoreCase(Constants.OUTPUT_FORMAT_NTRIPLE)) {
      val materializer = new NTripleMaterializer(model, writer, outputStream);
      materializer
    } else if(rdfLanguage.equalsIgnoreCase(Constants.OUTPUT_FORMAT_RDFXML)) {
      val materializer = new RDFXMLMaterializer(model, writer, outputStream);
      materializer
    } else {
      val materializer = new NTripleMaterializer(model, writer, outputStream);
      materializer
    }
  }
}