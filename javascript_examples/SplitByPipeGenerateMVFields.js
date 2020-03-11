function(doc){

    var directed_by = doc.getFirstFieldValue('directed_by_t');
    if(directed_by){
    if(directed_by.indexOf('|')>0){
       var directors = directed_by.split('|');

        for(var i=0; i<directors.length; i++){
         doc.addField('directed_by_tt', directors[i]);
         }
       }
    }

    var genres = doc.getFirstFieldValue('genre_t');
    if(genres){
        if(genres.indexOf('|')>0){
            var allGenres = genres.split('|');
            for(var i=0; i<allGenres.length; i++){
                doc.addField('genre_tt', allGenres[i]);
            }
        }
    }
    return doc;
  }