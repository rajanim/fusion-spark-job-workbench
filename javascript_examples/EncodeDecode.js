function (doc, ctx) {
   //get the encoded field value
   var encodedString = doc.getFirstFieldValue("url").toString();
   //get decode libraries
   var Base64 = Java.type('java.util.Base64');
   var JavaString = Java.type('java.lang.String');
   var decoder = Base64.getDecoder();
   //decode
   var decodeBytes = decoder.decode(encodedString)
   var decodedString = new JavaString(decodedBytes);

  doc.addField("decodedString_s", decodedString);
  //encode
  var encoder = Base64.getEncoder();
  var encodedString = encoder.encodeToString(new JavaString(docId).getBytes("UTF-8"));
  // if you'd like to reset a field value - doc.setId(docId);
  //add encoded value as new field
  doc.addField("encodedId", encodedString);
  return doc;
}

function (doc, ctx) {
   //get the encoded field value
   var title = doc.getFirstFieldValue("aero_title_t").toString();
   //get decode libraries
   var Base64 = Java.type('java.util.Base64');
   var JavaString = Java.type('java.lang.String');
   //decode
   var decoder = Base64.getDecoder();
   var decodeBytes = decoder.decode(title)
   var decodedTitle = new JavaString(decodedBytes);

   doc.addField("decodedTitle_t", decodedTitle);
   // if you'd like to reset the same title field value - doc.setField(decodedTitle);
  return doc;
}