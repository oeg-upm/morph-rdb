package es.upm.fi.dia.oeg.morph.base

import com.google.common.base.Charsets
import com.google.common.io.BaseEncoding
import com.mashape.unirest.http.Unirest
import org.json.JSONObject
import java.net.HttpURLConnection
import org.slf4j.LoggerFactory

/**
  * Created by fpriyatna on 21/02/2017.
  */
class GitHubUtility {

}

object GitHubUtility {
val logger = LoggerFactory.getLogger(this.getClass());
  
  
  def getRawURLFromBlobURL(blobURL:String) : String = {
    val fileAPIURL = blobURL
      .replaceAllLiterally("https://github.com/", "https://raw.githubusercontent.com/")
      .replaceAllLiterally("/blob/master/", "/master/");
    return fileAPIURL; 
  }


  def encodeToBase64(content:String) : String = {
    val base64EncodedContent = BaseEncoding.base64().encode(content.getBytes(Charsets.UTF_8));
    base64EncodedContent;
  }

  def decodeFromBase64(encodedContent:String) : String = {
    val cleanedEncodedString = encodedContent.replaceAllLiterally("\n", "");
    val base64DecodedContent = BaseEncoding.base64().decode(cleanedEncodedString);
    val decodedContentInString = new String(base64DecodedContent, Charsets.UTF_8);
    decodedContentInString;
  }
  
  def getSHA(mappingpediaUsername:String, mappingDirectory:String, mappingFilename:String
             , githubUsername:String, githubAccessToken:String) : String = {
    val uri = "https://api.github.com/repos/oeg-upm/mappingpedia-contents/contents/{mappingpediaUsername}/{mappingDirectory}/{mappingFilename}";
    val response = Unirest.get(uri)
      .routeParam("mappingpediaUsername", mappingpediaUsername)
      .routeParam("mappingDirectory", mappingDirectory)
      .routeParam("mappingFilename", mappingFilename)
      //.routeParam("mappingFileExtension", mappingFileExtension)
      .basicAuth(githubUsername, githubAccessToken)
      .asJson();

    response.getBody.getObject.getString("sha");
  }

  def getSHA(url:String, githubUsername:String, githubAccessToken:String) : String = {
    val response = Unirest.get(url)
      .basicAuth(githubUsername, githubAccessToken)
      .asJson();
    response.getBody.getObject.getString("sha");
  }
}
