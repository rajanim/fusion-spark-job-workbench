//From : 26/April/2019:14:14:14
//To : 2016-11-01T20:20:20Z

function(doc){
if(doc.getId() ! == null){
  var inBoundPattern = "dd/MMM/yyyy':'HH:mm:ss Z";
  var solrDatePattern = "yyyy-MM-dd'T'HH:mm:ss'Z'";

  var dateParser = new java.text.SimpleDateFormat(inBoundPattern);
  var solrFormatter = new java.text.SimpleDateFormat(solrDatePattern);

  var date_s = doc.getFirstFieldValue("date_s");
  var parsedDate = dateParser.parse(date_s);
  var formattedDate = solrFormatter.format(parsedDate);
  doc.setField("date_time_tdt",formattedDate);

}

return doc;

}