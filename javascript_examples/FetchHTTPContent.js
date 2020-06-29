function(doc){
    logger.info('gdelt indexing--------in function:  doc: hasfield SOURCEURL' + doc.hasField('SOURCEURL')  );
	  var JavaString = Java.type('java.lang.String');
    if (doc && doc.hasField('SOURCEURL_s')) {
		var url = doc.getFirstFieldValue("SOURCEURL_s");
		var entityUtil = Java.type('org.apache.http.util.EntityUtils');

		logger.info('gdelt indexing--------url field is :' + url);
		try {
		  //sample http://ip.jsontest.com/
		 if (url) {
			var httpResp = queryHttp(url);

			// Check the response
			var statusCode = httpResp.getStatusLine().getStatusCode();

			if (statusCode < 200 || statusCode >= 300) {
				logger.warn("gdelt indexing -------- HTTPS Response code from news fetch = " + statusCode + ": " + httpResp.getStatusLine().getReasonPhrase());
				return null;
			}
			logger.info("gdelt indexing-------- JSON:"+httpResp);
			//logger.info("PDF Content:"+httpResp.getEntity().getContent());
			//logger.info("PDF byte array:"+entityUtil.toByteArray(httpResp.getEntity()));
			var entity = httpResp.getEntity();

			logger.info("gdelt indexing--------Entity:"+entity);
			if(entity){
			  //doc.setField('_fetchResult_s',httpResp);
			  //doc.setField('_raw_content_', httpResp);
			  doc.setField('_raw_content_', entityUtil.toByteArray(entity));
			}

		  }
		}catch(error){
		  logger.error("gdelt indexing--------Error handling url '" + url + "' Err: " + error);
		}
		return doc;
	}
    return doc;
}

function queryHttp(url){

    var HttpClientBuilder = Java.type('org.apache.http.impl.client.HttpClientBuilder');
    var HttpGet = Java.type('org.apache.http.client.methods.HttpGet');
    var HttpHost = Java.type('org.apache.http.HttpHost');

    logger.info('gdelt indexing-------- BUILDING HTTP REQUEST : ' );
    var client = HttpClientBuilder.create().build();
    var request = new HttpGet(url);
    logger.info('gdelt indexing--------SUBMITTING HTTP REQUEST : ' + request);
    var rsp = client.execute(request);

    return rsp;
}


