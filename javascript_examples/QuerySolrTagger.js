function(request, response, ctx, collection, solrServer, solrServerFactory){

if(ctx.get('debug')){
logger.info('query solr tagger stage'+ ctx.toString());
}
var q = (ctx.get('q')=='_null_'? null : ctx.get('q'));


var HttpPost = Java.type('org.apache.http.client.methods.HttpPost');
var CloseableHttpClient = Java.type('org.apache.http.impl.client.CloseableHttpClient');
var HttpClientBuilder = Java.type('org.apache.http.impl.client.HttpClientBuilder');
var ContentType = Java.type('org.apache.http.entity.ContentType');
var StringEntity = Java.type('org.apache.http.entity.StringEntity');
var EntityUtils = Java.type('org.apache.http.utils.EntityUtils');


var tagCollection = 'analysts';

if(q){

 var url = 'http://localhost:8983/solr/'+ tagCollection + '/tag?overlaps=LONGEST_DOMINANT_RIGHT&tagsLimit=10&fl=id,name_tag&matchText=true&wt=json'
 var post = new HttpPost(url);
 post.setEntity(new StringEntity(q, ContentType.TEXT_PLAIN));
 var client = HttpClientBuilder.create().build();
 var response = client.execute(post);

if(response){

  var entity = response.getEntity();
  if(entity){
   var entityText = EntityUtils.toString(entity, 'UTF-8');
   logger.info('Solr tagger results: ' + entityText);
   request.addParam('tags result', entityText);

   var resp = JSON.parse(entityText);
   if(resp){
   var taggerEntityQuery;
   if(resp.response.docs.length>0){
        var responseDocs = resp.response.docs;
        taggerEntityQuery = "";
        var prevComponentType;
        var idQuery = "";

        for(var i=0; i< responseDocs.length; i++){
        var doc = responseDocs[i];
        var componentType= doc['common_component_type_s'];


        }

       }
     }
   }

 }
   else{
    logger.error('no response received from tagger end point' + url);

 }

 }

}

/*
curl -XPOST -H 'Content-type:application/json'  'http://localhost:8983/solr/Films/schema' -d '{
  "add-field-type":{
    "name":"tag",
    "class":"solr.TextField",
    "postingsFormat":"FST50",
    "omitNorms":true,
    "omitTermFreqAndPositions":true,
    "indexAnalyzer":{
      "tokenizer":{ "class":"solr.StandardTokenizerFactory" },
      "filters":[
        {"class":"solr.LowerCaseFilterFactory"},
        {"class":"solr.ConcatenateGraphFilterFactory", "preservePositionIncrements":false }
      ]},
    "queryAnalyzer":{
      "tokenizer":{
         "class":"solr.StandardTokenizerFactory" },
      "filters":[
        {"class":"solr.LowerCaseFilterFactory"}
      ]}
    },
  "add-field":{"name":"name_tag", "type":"tag", "stored":true},
  "add-copy-field":{"source":"directed_by_s", "dest":["name_tag"]},
    "add-copy-field":{"source":"name_s", "dest":["name_tag"]}*/
