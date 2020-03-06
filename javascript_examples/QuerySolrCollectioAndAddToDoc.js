function(doc){
//solrj client apis to query solr and get necessary docs/fields 
var solrQuery =Java.type('org.apache.solr.client.solrj.SolrQuery');
//we could make use of solrj solr client or use http client 
var httpPost = Java.type('org.apache.http.client.methods.HttpPost');
var httpClientBuilder = Java.type('org.apache.http.impl.client.HttpClientBuilder');
var contentType = Java.type('org.apache.http.entity.ContentType');
var entityUtils = Java.type('org.apache.http.util.EntityUtils');
var stringEntity = Java.type('org.apache.http.entity.StringEntity');
//collection name to query
var tagsCollectionName= 'ANETTIL';
//get id
var id = doc.getFirstFieldValue('fusion_query_id');

var url = 'http://localhost:8983/solr/' + tagsCollectionName + '/select?q=fusion_query_id:' + id;
var post = new httpPost(url);
post.setEntity(new stringEntity(terms, ContentType.Text_Plain));

var client = httpClientBuilder.create().build();
var response = client.execute(post);

if(response){
    var entity = response.getEntity();
    if(entity){
        var entityText = entityUtils.toString(entity, 'UTF-8');
        logger.info('Results' + entityText);
    }
}

    return doc;
}