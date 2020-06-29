function (doc, ctx) {
  // Add a "Delete" command, which will delete the document from Solr
  if (doc && doc.getId()) {
    var deleteDocument = Java.type("com.lucidworks.apollo.common.pipeline.PipelineDocument").newDelete(doc.getId(), true);
    ctx.put("isDelete", "true");
    return deleteDocument;
  }
  return doc;
}