import static java.lang.String.format;
import static org.apache.jena.fuseki.Fuseki.serverLog;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.web.ContentType;
import org.apache.jena.fuseki.FusekiLib;
import org.apache.jena.fuseki.HttpNames;
import org.apache.jena.fuseki.server.DatasetRef;
import org.apache.jena.fuseki.servlets.HttpAction;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.WebContent;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFLib;
import org.apache.jena.web.HttpSC;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.compose.Difference;
import com.hp.hpl.jena.graph.compose.Union;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.DatasetImpl;
import com.hp.hpl.jena.sparql.graph.GraphFactory;


public class EventSourced_REST_RW extends EventSourced_REST_R {

    @Override
    protected void doOptions(HttpAction action)
    {
        //action.response.setHeader(HttpNames.hAllow, "GET,HEAD,OPTIONS,PUT,DELETE,POST");
    	action.response.setHeader(HttpNames.hAllow, "GET,HEAD,OPTIONS,PUT,POST");
        action.response.setHeader(HttpNames.hContentLengh, "0") ;
        success(action) ;
    }
    
    @Override
    protected void doPut(HttpAction action)     { doPutPost(action, true) ; }

    @Override
    protected void doPost(HttpAction action)     { doPutPost(action, false) ; }

    private void doPutPost(HttpAction action, boolean overwrite) {
        ContentType ct = FusekiLib.getContentType(action) ;
        if ( ct == null )
            errorBadRequest("No Content-Type") ;

        // Helper case - if it's a possible HTTP file upload, pretend that's the action.
//        if ( WebContent.contentTypeMultipartFormData.equalsIgnoreCase(ct.getContentType()) ) {
//            String base = wholeRequestURL(action.request) ;
//            SPARQL_Upload.upload(action, base) ;
//            return ; 
//        }

        if ( WebContent.matchContentType(WebContent.ctMultipartMixed, ct) )
            error(HttpSC.UNSUPPORTED_MEDIA_TYPE_415, "multipart/mixed not supported") ;
        
//        boolean existedBefore = false ;
//        if ( action.isTransactional() )
//            existedBefore = addDataIntoTxn(action, overwrite) ;
//        else
//            existedBefore = addDataIntoNonTxn(action, overwrite) ;
//            
//        if ( existedBefore )
//            ServletBase.successNoContent(action) ;
//        else
//            ServletBase.successCreated(action) ;
        DatasetRef descInternal = action.dsRef;
		DatasetGraph dsgInternal = action.dsRef.dataset;
		String a = "http://test.drugis.org/person/Gert";
		String t = now();

		serverLog.info(format("REST_R %s %s %s", action.dsRef, a, t));
		
		Dataset ds = EventSource.createView(DatasetImpl.wrap(dsgInternal), a, t);
		DatasetRef desc = new DatasetRef();
		desc.dataset = ds.asDatasetGraph();
		serverLog.info(desc.dataset.toString());
		action.setDataset(desc);
		
		// First: determine the differences
		
		action.beginRead();

        Target target = determineTarget(action);
        
        Graph claims = null;
        Graph retractions = null;
	    Graph g = GraphFactory.createDefaultGraph();
	    StreamRDF sink = StreamRDFLib.graph(g);
	    incomingData(action, sink);
	    serverLog.info(format("Exists: %b Overwrite: %b", target.exists(), overwrite));
	    if (!target.exists() || !overwrite) {
	    	claims = g;
	    } else {
	    	claims = new Difference(g, target.graph());
	    	retractions = new Difference(target.graph(), g);
	    }
	    serverLog.info(format("CLAIMS: \n%s\nRETRACTIONS: %s", claims, retractions));

	    action.endRead();
	    
	    // Second: write the delta
	    
	    action.setDataset(descInternal);
	    action.beginWrite();

    	String deltaId = randomId();
		Node claimsGraphName = NodeFactory.createURI("http://test.drugis.org/claim/" + deltaId);
		Node retractionsGraphName = NodeFactory.createURI("http://test.drugis.org/retract/" + deltaId);
	    
	    String stmt = "http://test.drugis.org/ontology/statements#";
	    Node deltaURI = NodeFactory.createURI("http://test.drugis.org/delta/" + deltaId);
	    Node metaGraphName = NodeFactory.createURI("http://test.drugis.org/meta");
		Graph meta = dsgInternal.getGraph(metaGraphName);

		String rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
		String dc = "http://purl.org/dc/elements/1.1/";
		meta.add(Triple.create(deltaURI, NodeFactory.createURI(rdf + "type"), NodeFactory.createURI(stmt + "Delta")));
		meta.add(Triple.create(deltaURI, NodeFactory.createURI(stmt + "about"), NodeFactory.createURI(action.getRequest().getParameter("graph"))));
		meta.add(Triple.create(deltaURI, NodeFactory.createURI(dc + "creator"), NodeFactory.createURI(a)));
		meta.add(Triple.create(deltaURI, NodeFactory.createURI(dc + "date"), NodeFactory.createLiteral(t, XSDDatatype.XSDdateTime)));
	    if (claims != null) {
			dsgInternal.addGraph(claimsGraphName, claims);
			meta.add(Triple.create(deltaURI, NodeFactory.createURI(stmt + "claims"), claimsGraphName));
	    }
	    if (retractions != null) {
			dsgInternal.addGraph(retractionsGraphName, retractions);
			meta.add(Triple.create(deltaURI, NodeFactory.createURI(stmt + "retractions"), retractionsGraphName));
	    }
	    action.commit();
	    
	    action.endWrite();
    }

    private static void incomingData(HttpAction action, StreamRDF dest) {
        String base = wholeRequestURL(action.request) ;
        ContentType ct = FusekiLib.getContentType(action) ;
        Lang lang = RDFLanguages.contentTypeToLang(ct.getContentType()) ;
        if ( lang == null ) {
            errorBadRequest("Unknown content type for triples: " + ct) ;
            return ;
        }
        InputStream input = null ;
        try { input = action.request.getInputStream() ; } 
        catch (IOException ex) { IO.exception(ex) ; }
    
        int len = action.request.getContentLength() ;
        if ( action.verbose ) {
            if ( len >= 0 )
                log.info(format("[%d]   Body: Content-Length=%d, Content-Type=%s, Charset=%s => %s", action.id, len,
                                ct.getContentType(), ct.getCharset(), lang.getName())) ;
            else
                log.info(format("[%d]   Body: Content-Type=%s, Charset=%s => %s", action.id, ct.getContentType(),
                                ct.getCharset(), lang.getName())) ;
        }
    
        parse(action, dest, input, lang, base) ;
    }

    private SecureRandom random = new SecureRandom();

    private String randomId() {
        return new BigInteger(130, random).toString(32);
    }
    
    // http://stackoverflow.com/questions/3914404
	private String now() {
	    TimeZone tz = TimeZone.getTimeZone("UTC");
	    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	    df.setTimeZone(tz);
	    String nowAsISO = df.format(new Date());
	    return nowAsISO;
	}
}
