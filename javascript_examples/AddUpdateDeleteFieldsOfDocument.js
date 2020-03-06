function(doc){
var type;
//check if the field "type" exists
if(doc.hasField('type')){
    //get the value of the field
    type = doc.getFirstFieldValue('type');
    //if the value is matching the condition
    if(type=='click'){
        //remove some field, "test" field in this case
        doc.removeFields('test');
        //add some field, "count_i" in this case
        doc.setField('count_i',1);
    }
}
//make sure this "return" is returning the doc
    return doc;
}