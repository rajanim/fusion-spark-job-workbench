function(doc){


var HttpClientBuilder = Java.type('org.apache.http.impl.client.HttpClientBuilder');
var HttpGet = Java.type('org.apache.http.client.methods.HttpGet');
var IOUtils = Java.type('org.apache.commons.io.IOUtils');
var HttpHost = Java.type('org.apache.http.HttpHost');
var Paths =  Java.type('java.nio.files.Paths');
var Files = Java.type('java.nio.file.Files');
var String = Java.type('java.lang.String');
var StandardCharsets = Java.type('java.nio.charset.StandardCharsets')


var docId = doc.getFirstFieldValue("Doc_ID");
if(docId==null)
return null;

var getRestUrl = "http://localhost:8080/001/doc_001.xml";
logger.info("url: " + getRestUrl);

var builder = HttpClientBuilder.create();
var getRequest = new HttpGet(getRestUrl);
var getClient = builder.build();
var responseString = "";
var getResponse = getClient.execute(getRequest);

if(getResponse && getResponse.getEntity()){
responseString = IOUtils.toString(getResponse.getEntity().getContent(), 'UTF-8');

//write to file
Files.write("", responseString.getBytes(StandardCharsets.UTF_8));

doc.setField('responseText_txt', responseString);

return doc;
}


}