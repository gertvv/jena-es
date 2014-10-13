package fuseki;
import org.apache.jena.fuseki.servlets.HttpAction;
import org.apache.jena.fuseki.servlets.SPARQL_QueryDataset;

import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.Query;

import es.DatasetGraphEventSourcing;

public class EventSourced_QueryDataset extends SPARQL_QueryDataset {
	private static final long serialVersionUID = -6181438945345103279L;
	
	public EventSourced_QueryDataset() {
		super();
	}

	@Override
    protected Dataset decideDataset(HttpAction action, Query query, String queryStringLog) 
    {
        Dataset es = super.decideDataset(action, query, queryStringLog);
		DatasetGraphEventSourcing dsg = (DatasetGraphEventSourcing) es.asDatasetGraph();
		String eventId = action.request.getParameter("event");

        if (eventId != null) { // return a specific version
    		Dataset ds = DatasetFactory.create(dsg.getView(NodeFactory.createURI(eventId)));
        	System.err.println("Returning version " + eventId);
    		return ds;
        }
        
        System.err.println("Returning version CURRENT");
        return es; // return the current version
    }

	
}
