package fuseki;

import java.util.Enumeration;

import javax.servlet.http.HttpServletRequest;

import org.apache.jena.fuseki.HttpNames;
import org.apache.jena.fuseki.server.DatasetRef;
import org.apache.jena.fuseki.servlets.HttpAction;
import org.apache.jena.fuseki.servlets.SPARQL_REST_R;

import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.sparql.core.DatasetGraph;

import es.DatasetGraphEventSourcing;
import es.EventSource.EventNotFoundException;

public class EventSourced_REST_R extends SPARQL_REST_R {
	private static final long serialVersionUID = 4361496160143818910L;

	@Override
	public void doGet(HttpAction action) {
		DatasetGraph dsg = getDatasetGraph(action);
		DatasetRef desc = new DatasetRef();
		desc.dataset = dsg;
		action.setDataset(desc);
		super.doGet(action);
	}

	private DatasetGraph getDatasetGraph(HttpAction action) {
		DatasetGraphEventSourcing es = (DatasetGraphEventSourcing) action.dsRef.dataset;
		String eventId = action.request.getParameter("event");

        if (eventId != null) { // return a specific version
        	DatasetGraph dsg = null;
        	try {
        		dsg = es.getView(NodeFactory.createURI(eventId));
        	} catch (EventNotFoundException e) {
        		errorNotFound(e.getMessage());
        	}
        	System.err.println("Returning version " + eventId);
        	return dsg;
        }
        
        System.err.println("Returning version CURRENT");
        return es; // return the current version
	}
	
	// copy/paste from SPARQL_Protocol
    protected static int countParamOccurences(HttpServletRequest request, String param)
    {
        String[] x = request.getParameterValues(param) ;
        if ( x == null )
            return 0 ;
        return x.length ;
    }
    
    // copy/paste and adjusted from SPARQL_REST
    @Override
    protected void validate(HttpAction action)
    {
        HttpServletRequest request = action.request ;
        // Direct naming.
        if ( request.getQueryString() == null )
            //errorBadRequest("No query string") ;
            return ;
        
        String g = request.getParameter(HttpNames.paramGraph) ;
        String d = request.getParameter(HttpNames.paramGraphDefault) ;
        
        if ( g != null && d !=null )
            errorBadRequest("Both ?default and ?graph in the query string of the request") ;
        
        if ( g == null && d == null )
            errorBadRequest("Neither ?default nor ?graph in the query string of the request") ;
        
        int x1 = countParamOccurences(request, HttpNames.paramGraph) ;
        int x2 = countParamOccurences(request, HttpNames.paramGraphDefault) ;
        
        if ( x1 > 1 )
            errorBadRequest("Multiple ?default in the query string of the request") ;
        if ( x2 > 1 )
            errorBadRequest("Multiple ?graph in the query string of the request") ;
        
        Enumeration<String> en = request.getParameterNames() ;
        for ( ; en.hasMoreElements() ; )
        {
            String h = en.nextElement() ;
            if ( ! HttpNames.paramGraph.equals(h) && ! HttpNames.paramGraphDefault.equals(h) && ! "event".equals(h) )
                errorBadRequest("Unknown parameter '"+h+"'") ;
            // one of ?default and &graph
            if ( request.getParameterValues(h).length != 1 )
                errorBadRequest("Multiple parameters '"+h+"'") ;
        }
    }
}
