import java.io.IOException;
import java.nio.charset.Charset;

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


public class EventSource {
	
	public static Model getHistory(Dataset eventSource, String g, String a) {
		if (eventSource == null) {
			throw new NullPointerException("Event Source may not be null");
		}
		if (g == null) {
			throw new NullPointerException("Graph may not be null");
		}
		if (a == null) {
			throw new NullPointerException("Agent may not be null");
		}
		
		try {
			String queryStr = HelloRDFWorld.readFile("graphDeltas.sparql", Charset.forName("UTF-8"));
//			ParameterizedSparqlString sparql = new ParameterizedSparqlString(); FIXME: use instead of String.replace
			queryStr = queryStr.replace("$author", "<" + a + ">");
			queryStr = queryStr.replace("$topic", "<" + g + ">");
			Query query = QueryFactory.create(queryStr);
			
			QueryExecution qe = QueryExecutionFactory.create(query, eventSource);
			Model resultModel = qe.execConstruct();
			qe.close();
			return resultModel;
		} catch (IOException e) {
			// FIXME
		}
		return null;
	}

	/**
	 * Construct an event sourced dataset.
	 * @param eventSource The underlying dataset representing the events
	 * @param a URI of the agent making the statements
	 * @param t String representing the time (XSD datetime) at which to construct the state of the database
	 * @return The dataset consisting of statements that were made by agent a by time t.
	 */
	public static Dataset createView(Dataset eventSource, String a, String t) {
		if (eventSource == null) {
			throw new NullPointerException("Event Source may not be null");
		}
		if (a == null) {
			throw new NullPointerException("Agent may not be null");
		}
		if (t == null) {
			throw new NullPointerException("Time may not be null");
		}
		
		try {
			String queryStr = HelloRDFWorld.readFile("findDeltas.sparql", Charset.forName("UTF-8"));
//			ParameterizedSparqlString sparql = new ParameterizedSparqlString(); FIXME: use instead of String.replace
			queryStr = queryStr.replace("$author", "<" + a + ">");
			queryStr = queryStr.replace("$t", "\"" + t + "\""  + "^^" + "<" + XSDDatatype.XSDdateTime.getURI() + ">");
			Query query = QueryFactory.create(queryStr);
			
			Dataset ds = new DatasetImpl(ModelFactory.createDefaultModel());
			
			try (QueryExecution qe = QueryExecutionFactory.create(query, eventSource)) {
				ResultSet results = qe.execSelect();
				Resource prevTopic = null;
				Graph graph = null;
				
				while (results.hasNext()) {
			        QuerySolution soln = results.next() ;
			        Resource topic = soln.getResource("topic");
			        Resource claims = soln.getResource("claims");
			        Resource rtrcts = soln.getResource("retractions");
			
			        if (!topic.equals(prevTopic)) {
			        	if (prevTopic != null) {
			        		Model model = ModelFactory.createModelForGraph(graph);
			        		ds.addNamedModel(prevTopic.getURI(), model);
			        	}
			        	prevTopic = topic;
			        	graph = GraphFactory.createDefaultGraph();
			        }
			        
			        if (rtrcts != null) {
			        	graph = (new Difference(graph, eventSource.getNamedModel(rtrcts.getURI()).getGraph()));
			        }
			        if (claims != null) {
			        	graph = new Union(graph, eventSource.getNamedModel(claims.getURI()).getGraph());
			        }
				}
				if (prevTopic != null) {
	        		Model model = ModelFactory.createModelForGraph(graph);
	        		ds.addNamedModel(prevTopic.getURI(), model);
				}
			}
			
			return ds;
		} catch (IOException e) {
			// FIXME
		}
		return null;
	}
}
