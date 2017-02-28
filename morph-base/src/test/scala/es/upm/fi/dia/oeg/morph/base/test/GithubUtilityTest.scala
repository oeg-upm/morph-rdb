package es.upm.fi.dia.oeg.morph.base.test

import es.upm.fi.dia.oeg.morph.base.GitHubUtility


object GithubUtilityTest extends App {
  val blobURL = "https://github.com/oeg-upm/mappingpedia-contents/blob/master/mappingpedia-testuser/8945c880-8a2a-4763-a1f9-3cdb84ac9643/mapping.ttl"
	//"https://api.github.com/repos/oeg-upm/mappingpedia-contents/contents/mappingpedia-testuser/8945c880-8a2a-4763-a1f9-3cdb84ac9643/mapping.ttl"
  //"https://api.github.com/repos/oeg-upm/mappingpedia-contents/contents/mappingpedia-testuser/8945c880-8a2a-4763-a1f9-3cdb84ac9643/mapping.ttl"
  
  
  val rawURL = GitHubUtility.getRawURLFromBlobURL(blobURL);
  printf("rawURL = " + rawURL);
}