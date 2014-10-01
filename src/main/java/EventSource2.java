import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.compose.Difference;
import com.hp.hpl.jena.graph.compose.Union;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.DatasetGraphFactory;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.graph.GraphFactory;
import com.hp.hpl.jena.vocabulary.RDF;


public class EventSource2 {
	public static final String ES="http://drugis.org/eventSourcing/es#",
			EVENT="http://drugis.org/eventSourcing/event/",
			REVISION="http://drugis.org/eventSourcing/revision/";
	public static final Node esClassLog = NodeFactory.createURI(ES + "Log"),
			esClassEvent = NodeFactory.createURI(ES + "Event"),
			esClassRevision = NodeFactory.createURI(ES + "Revision"),
			esPropertyHead = NodeFactory.createURI(ES + "head"),
			esPropertyGraph = NodeFactory.createURI(ES + "graph"),
			esPropertyRevision = NodeFactory.createURI(ES + "has_revision"),
			esPropertyAssertions = NodeFactory.createURI(ES + "assertions"),
			esPropertyRetractions = NodeFactory.createURI(ES + "retractions");
	
	public static DatasetGraph replayLog(DatasetGraph eventSource, Node log) {
		return replayLogUntil(eventSource, log, getLatestEvent(eventSource, log));
	}
	
	public static DatasetGraph replayLogUntil(DatasetGraph eventSource, Node log, Node event) {
		List<Node> events = getEventsUntil(eventSource, log, event);
		Collections.reverse(events);
		
		DatasetGraph ds = DatasetGraphFactory.createMem();
		for (Node e : events) {
			ds = applyEvent(eventSource, log, ds, e);
		}
		return ds;
	}
	
	public static Node getLatestEvent(DatasetGraph eventSource, Node log) {
		Node head = getUniqueObject(eventSource.find(log, log, esPropertyHead, Node.ANY));
		return getUniqueObject(eventSource.find(log, head, RDF.Nodes.first, Node.ANY));
	}
	
	/**
	 * Get a list of all events up to and including the given event.
	 * @param eventSource
	 * @param log
	 * @param event
	 * @return A list of events, new to old.
	 */
	public static List<Node> getEventsUntil(DatasetGraph eventSource, Node log, Node event) {
		List<Node> list = new ArrayList<Node>();
		boolean seen = false;
		
		Node current = getUniqueObject(eventSource.find(log, log, esPropertyHead, Node.ANY));
		while (!current.equals(RDF.Nodes.nil)) {
			Node el = getUniqueObject(eventSource.find(log, current, RDF.Nodes.first, Node.ANY));
			if (!seen && el.equals(event)) {
				seen = true;
			}
			if (seen) {
				list.add(el);
			}
			current = getUniqueObject(eventSource.find(log, current, RDF.Nodes.rest, Node.ANY));
		}

		return list;
	}
	
	private static Node getUniqueObject(Iterator<Quad> result) {
		if (result.hasNext()) {
			Node object = result.next().getObject();
			if (result.hasNext()) {
				throw new IllegalStateException("Multiple subjects on property of arity 1");
			}
			return object;
		}
		throw new IllegalStateException("Zero subjects on property of arity 1");
	}

	public static DatasetGraph applyEvent(DatasetGraph eventSource, Node log, DatasetGraph base, Node event) {
		DatasetGraph ds = DatasetGraphFactory.create(base);
		for (Iterator<Quad> quads = eventSource.find(log, event, esPropertyRevision, Node.ANY); quads.hasNext(); ) {
			Node revision = quads.next().getObject();
			Node graphName = getUniqueObject(eventSource.find(log, revision, esPropertyGraph, Node.ANY));
			Graph graph = applyRevision(eventSource, log, base.getGraph(graphName), revision);
			base.addGraph(graphName, graph);
		}
		return base;
	}
	
	private static Graph matchingGraph(DatasetGraph eventSource, Iterator<Quad> result) {
		if (result.hasNext()) {
			Graph graph = eventSource.getGraph(result.next().getObject());
			if(result.hasNext()) {
				throw new IllegalStateException("Multiple objects on property of arity 0/1");
			}
			return graph;
		}
		return GraphFactory.createGraphMem();
	}

	public static Graph applyRevision(DatasetGraph eventSource, Node log, Graph base, Node revision) {
		Graph additions = matchingGraph(eventSource, eventSource.find(log, revision, esPropertyAssertions, Node.ANY));
		Graph retractions = matchingGraph(eventSource, eventSource.find(log, revision, esPropertyRetractions, Node.ANY));
		return new Union(new Difference(base, retractions), additions);
	}
}
