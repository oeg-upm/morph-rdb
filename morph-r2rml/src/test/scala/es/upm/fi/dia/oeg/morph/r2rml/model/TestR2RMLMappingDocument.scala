package es.upm.fi.dia.oeg.morph.r2rml.model

object TestR2RMLMappingDocument extends App {
  println("Hello World");
  
  val rawURL = "https://raw.githubusercontent.com/oeg-upm/mappingpedia-contents/master/mappingpedia-testuser/95c80c25-7bff-44de-b7c0-3a4f3ebcb30c/95c80c25-7bff-44de-b7c0-3a4f3ebcb30c.ttl"
  val localPath = "mapping.ttl";
  val blobURL = "https://github.com/oeg-upm/mappingpedia-contents/blob/master/mappingpedia-testuser/07817037-5749-457d-9a0f-46b7a05c66aa/mapping.ttl";
  
  val md = R2RMLMappingDocument(localPath);
  println("md = " + md);
  println("Bye");
}

