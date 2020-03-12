
/*
- Stage in signals ingest pipeline
- get fusion_query_id
- query signals collection and get the above doc id
- add a field retrieved from this doc
- ingest it back into doc in pipeline
-

*/
function getAndUpdateRecord(doc, ctx, collection, solrServer, solrServerFactory) {

    //get the doc id of doc to retrieve
    var id = doc.getFirstFieldValue('id');
     //create solr query object
    var query = new org.apache.solr.client.solrj.SolrQuery();
    //set q param
    query.setQuery('id:' + id);

    //query for record(s)
    var solrSrvr = solrServerFactory.getSolrServer("Films");
    var records = solrSrvr.query(query).getResults();
    if(records.size()>0){
    //get the first records
      var record = records.get(0);
      //get the field value
      var query = record.getFieldValue('name_s');
      //add it to the doc in pipeline
      doc.setField('name_copied_t', query);
    }

return doc;

}