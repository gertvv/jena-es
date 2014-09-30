import static java.lang.String.format;

import java.io.IOException;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.jena.atlas.web.MediaType;
import org.apache.jena.atlas.web.TypedOutputStream;
import org.apache.jena.fuseki.HttpNames;
import org.apache.jena.fuseki.servlets.HttpAction;
import org.apache.jena.fuseki.servlets.SPARQL_ServletBase;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RDFWriterRegistry;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.sparql.core.DatasetImpl;


public class EventSourced_History extends SPARQL_ServletBase {

    protected void doGet(HttpServletRequest req, HttpServletResponse resp) {
    	doCommon(req, resp);
    }

	@Override
	protected void validate(HttpAction action) {
		
	}

	@Override
	protected void perform(HttpAction action) {
        if (action.getRequest().getMethod().equals(HttpNames.METHOD_GET))
            doGet(action);
	}

	private void doGet(HttpAction action) {
        MediaType mediaType = HttpAction.contentNegotationRDF(action) ;
        
        ServletOutputStream output ;
        try { output = action.response.getOutputStream() ; }
        catch (IOException ex) { errorOccurred(ex) ; output = null ; }
        
        TypedOutputStream out = new TypedOutputStream(output, mediaType) ;
        Lang lang = RDFLanguages.contentTypeToLang(mediaType.getContentType()) ;

        //if ( action.verbose )
            log.info(format("[%d]   Get: Content-Type=%s, Charset=%s => %s", 
                            action.id, mediaType.getContentType(), mediaType.getCharset(), lang.getName())) ;

        action.beginRead() ;

        try {
    		Model history = EventSource.getHistory(
    				DatasetImpl.wrap(action.getDatasetRef().dataset),
    				action.getRequest().getParameter("graph"),
    				"http://test.drugis.org/person/Gert");
            String ct = lang.getContentType().toHeaderString() ;
            action.response.setContentType(ct) ;

            //Special case RDF/XML to be the plain (faster, less readable) form
            RDFFormat fmt = 
                ( lang == Lang.RDFXML ) ? RDFFormat.RDFXML_PLAIN : RDFWriterRegistry.defaultSerialization(lang) ;  
            RDFDataMgr.write(out, history, fmt) ;
            success(action) ;
        } finally { action.endRead() ; }

		
	}

	
}
