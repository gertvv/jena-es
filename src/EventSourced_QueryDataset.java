import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.jena.fuseki.servlets.HttpAction;
import org.apache.jena.fuseki.servlets.SPARQL_QueryDataset;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.compose.Difference;
import com.hp.hpl.jena.graph.compose.Union;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.core.DatasetImpl;
import com.hp.hpl.jena.sparql.graph.GraphFactory;


public class EventSourced_QueryDataset extends SPARQL_QueryDataset {
	private static final long serialVersionUID = -6181438945345103279L;
	
	public EventSourced_QueryDataset() {
		super();
//		allParams.add("t");
	}

	@Override
    protected Dataset decideDataset(HttpAction action, Query query, String queryStringLog) 
    {
        Dataset datasetInternal = super.decideDataset(action, query, queryStringLog);
		String a = "http://test.drugis.org/person/Gert";
		String t = action.request.getParameter("t");
		
		Dataset ds = EventSource.createView(datasetInternal, a, t);
		return ds;
    }

	
}
