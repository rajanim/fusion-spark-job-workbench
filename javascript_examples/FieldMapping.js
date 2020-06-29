function(doc){
var mime_type_s;
if(doc.hasField('mime_type_s')){
    mime_type_s = doc.getFirstFieldValue('mime_type_s');
        doc.addField('Content-Type',mime_type_s);
        doc.setField('')
    }
    //make sure this "return" is returning the doc
        doc.setField('test_s', true);
        return doc;

}
