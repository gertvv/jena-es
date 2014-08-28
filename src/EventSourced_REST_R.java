import static java.lang.String.format;
import static org.apache.jena.fuseki.Fuseki.serverLog;

import java.util.Enumeration;

import javax.servlet.http.HttpServletRequest;

import org.apache.jena.fuseki.HttpNames;
import org.apache.jena.fuseki.server.DatasetRef;
import org.apache.jena.fuseki.servlets.HttpAction;
import org.apache.jena.fuseki.servlets.SPARQL_REST_R;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.DatasetImpl;


public class EventSourced_REST_R extends SPARQL_REST_R {

	@Override
	public void doGet(HttpAction action) {
		DatasetGraph dsgInternal = action.dsRef.dataset;
		String a = "http://test.drugis.org/person/Gert";
		String t = action.request.getParameter("t");

		serverLog.info(format("REST_R %s %s %s", dsgInternal, a, t));

		Dataset ds = EventSourced_QueryDataset.createEventSourcedDataset(DatasetImpl.wrap(dsgInternal), a, t);
		DatasetRef desc = new DatasetRef();
		desc.dataset = ds.asDatasetGraph();
		action.setDataset(desc);
		
		super.doGet(action);
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
            if ( ! HttpNames.paramGraph.equals(h) && ! HttpNames.paramGraphDefault.equals(h) && ! "t".equals(h) )
                errorBadRequest("Unknown parameter '"+h+"'") ;
            // one of ?default and &graph
            if ( request.getParameterValues(h).length != 1 )
                errorBadRequest("Multiple parameters '"+h+"'") ;
        }
    }
}
