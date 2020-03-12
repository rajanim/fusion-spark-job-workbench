
/*
 get the list of links in uri field
 get the solrServer instance with collection
 per link, delete the record in the collection
*/
function fusionDeleteById(doc, ctx, collection, solrServer, solrServerFactory) {
    var urls = doc.getFieldValues('uri');
    var solrSrvr = solrServerFactory.getSolrServer("web_collection");
    if( solrSrvr != null ){
        for(i = 0; i < urls.length; i++){
            var link = urls[i];
            logger.info('*** Link = ' + link);
            if(link != null){
            var res = solrSrvr.deleteById(link,10000);
            }
        }
    }
    return null;
}