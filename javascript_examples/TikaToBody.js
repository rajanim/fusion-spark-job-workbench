function(doc) {
    var Tika = org.apache.tika.Tika;
    var Metadata = org.apache.tika.metadata.Metadata;
    var AutoDetectParser = org.apache.tika.parser.AutoDetectParser;
    var ParseContext = org.apache.tika.parser.ParseContext;
    var OOXMLParser = org.apache.tika.parser.microsoft.ooxml.OOXMLParser;
    var PDFParser = org.apache.tika.parser.pdf.PDFParser;
    var BodyContentHandler = org.apache.tika.sax.BodyContentHandler;
    var String = java.lang.String;
    var URL = java.net.URL;
    var base64 = java.util.Base64;
    var ioe = java.io.IOException;

    var url = 'file:///' + doc.getFirstFieldValue('FILE_LOCATION');
    doc.addField('FILE_LINK',url);
    
    var autoParser = new AutoDetectParser();
    var tika = new Tika();

    var pdfParser = new PDFParser();
    var textHandler = new BodyContentHandler(-1);
    var metadata = new Metadata();
    var context = new ParseContext();
    var content = new String();
    
    try {
        var urlobj = new URL(url);
        var input = urlobj.openStream();

        if ("application/pdf".equals(tika.detect(urlobj))) {
            pdfParser.parse(input, textHandler, metadata, context);
            content = textHandler.toString();
        } else if ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet".equals(tika.detect(urlobj))) {
            var msofficeparser = new OOXMLParser();
            msofficeparser.parse(input, textHandler, metadata, context);
            content = textHandler.toString();
        } else {
            autoParser.parse(input, textHandler, metadata, context);
            content = textHandler.toString();
        }
        if (content !== null) {
            var encoder = base64.getEncoder();
            var encoded = encoder.encodeToString(content.getBytes());
           if(!doc.hasField("_raw_content_")){
             doc.addField("_raw_content_", encoded);
           } else {
            doc.setField("_raw_content_", encoded);
           }
        }
    } catch (ioe) {
        logger.error(ioe);
    }

    return doc;
}