function (req, res, ctx) {
 //define local  q variable with default
   var q = '*:*';
   //check if the request has "q" otherwise assign q with *
   if (!req.hasParam('q')) {
     req.putSingleParam('q', q);
     logger.info('No q param');
     return;
   }
   // get query param q and lowercase it, check if it is * then skip otherwise continue
   q = req.getFirstParam('q').trim().toLowerCase();
   if (q == '*:*') {
     logger.info('query param, q=*:*');
     return;
   }
   //allow user to do double quote search through out the corpus.
     var hasQuotedText = q.match(/^(\s)?"(.*)"(\s)?$/);
       if(hasQuotedText){
        newQ = '_text_:("' + q + '")';
        logger.info('query stage newQ =' + newQ);
           req.removeParam('q');
           req.removeParam('uf');
           req.addParam('newQ', newQ);
           req.putSingleParam('q', newQ);
        return;
       }
 //escape special characters in query to ensure it does not break query parser
   var specialCharacters = ["+","-","&&","||","!","(",")","{","}","[","]","^","\"","~","*","?",":","/"];
     for(var i=0; i<specialCharacters.length;i++){
         var escapeChar = "\\" + specialCharacters[i];
         q = q.replace(specialCharacters[i], escapeChar);
     }
     req.addParam("escapedString", q);
    var newQ = '';
  // generic search - boost by title, body, author, doc_type
  //create required terms query to support "mm" 100%  sort of search
   var termsRequired = '';
   if(q){
    var queryTerms = q.split(' ');
      for(var i=0; i< queryTerms.length; i++){
         termsRequired += "+" + queryTerms[i].trim() +" ";
         }
     termsRequired = termsRequired.trim();
       }
       newQ = 'aero_title_t:("' + q + '")^25';
       newQ +=  ' OR aero_title_t:(' + termsRequired + ')^20';
       newQ += ' OR aero_body_t:(' + termsRequired + ')^5';
       newQ +=  ' OR aero_body_t:("' + q + '")^10';
       newQ += ' OR _text_:(' + termsRequired + ')';
     //  newQ += ' OR aero_title_t:(' + q + ')';
     //  newQ += ' OR aero_body_t:(' + q + ')';
   //uncomment it if you'd want to search
    //   newQ += ' OR aero_author_t:(' + termsRequired + ')';
     //  newQ += ' OR aero_doc_type_t:(' + termsRequired + ')';
     logger.info('query stage newQ =' + newQ);
     req.removeParam('q');
     req.addParam('newQ', newQ);
     req.putSingleParam('q', newQ);
     //remove uf otherwise it breaks edismax parser flow
     req.removeParam('uf');
   return;
 }