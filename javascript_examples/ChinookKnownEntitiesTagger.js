function(request, response, ctx, collection, solrServer, solrServerFactory){

    var q = (request.getFirstParam('q')=='_null_'? null : request.getFirstParam('q'));
    logger.info("q: "+ q);
    if(q){
    var HttpPost = Java.type('org.apache.http.client.methods.HttpPost');
    var CloseableHttpClient = Java.type('org.apache.http.impl.client.CloseableHttpClient');
    var HttpClientBuilder = Java.type('org.apache.http.impl.client.HttpClientBuilder');
    var ContentType = Java.type('org.apache.http.entity.ContentType');
    var StringEntity = Java.type('org.apache.http.entity.StringEntity');
    var EntityUtils = Java.type('org.apache.http.util.EntityUtils');
    var HttpEntity = Java.type('org.apache.http.HttpEntity');
    var InputStrem = Java.type('java.io.InputStream');
    var IOUtils = Java.type('org.apache.commons.io.IOUtils');
    var tagCollection = 'Chinook';

    var url = 'http://localhost:8983/solr/'+ tagCollection + '/tag?overlaps=LONGEST_DOMINANT_RIGHT&tagsLimit=10&fl=id,name_tag&matchText=true&wt=json'
 	var post = new HttpPost(url);
 	post.setEntity(new StringEntity(q, ContentType.TEXT_PLAIN));
 	var client = HttpClientBuilder.create().build();
 	var response = client.execute(post);
      if(response){
      var entity = response.getEntity();
        if(entity) {
        var entityText = EntityUtils.toString(entity, 'UTF-8');
          request.addParam('hasTags', entityText);
            var resp = JSON.parse(entityText);
           /*  if(resp){
               if(resp.response.docs.length>0){
                var responseDocs = resp.response.docs;
                request.addParam("docs", responseDocs.length);
                      }
               }*/
        }

      }
    }
  return;

}

function (doc) {

    var HttpClientBuilder = Java.type('org.apache.http.impl.client.HttpClientBuilder');
    var HttpGet = Java.type('org.apache.http.client.methods.HttpGet');
    var StringEntity = Java.type('org.apache.http.entity.StringEntity');
    var EntityUtils = Java.type('org.apache.http.util.EntityUtils');
    var HttpEntity = Java.type('org.apache.http.HttpEntity');
    var InputStrem = Java.type('java.io.InputStream');
    var IOUtils = Java.type('org.apache.commons.io.IOUtils');

    var myRequest = new HttpGet(""+docUrl+"");

    var client = HttpClientBuilder.create().build();

    var rsp = client.execute(myRequest);
    //logger.info(rsp.getStatusLine().getStatusCode());
    HttpEntity = rsp.getEntity();


    if (HttpEntity != null) {
      var len = HttpEntity.getContentLength();
      doc.addField('content_length', len);

      var inputStrem = HttpEntity.getContent();
      doc.addField('InputStream', inputStrem);

      var bytearray = [];
      var byte = Java.type("byte[]");
      var bytearray = IOUtils.toByteArray(inputStrem);

    }
 return doc;
}
