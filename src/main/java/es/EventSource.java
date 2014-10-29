package es;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.GraphExtract;
import com.hp.hpl.jena.graph.GraphUtil;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleBoundary;
import com.hp.hpl.jena.graph.compose.Delta;
import com.hp.hpl.jena.graph.compose.Difference;
import com.hp.hpl.jena.graph.compose.Union;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.DatasetGraphFactory;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.core.Transactional;
import com.hp.hpl.jena.sparql.graph.GraphFactory;
import com.hp.hpl.jena.vocabulary.RDF;

@SuppressWarnings("deprecation")
public class EventSource {
	public static final String ES="http://drugis.org/eventSourcing/es#",
			EVENT="http://drugis.org/eventSourcing/event/",
			REVISION="http://drugis.org/eventSourcing/revision/",
			ASSERT="http://drugis.org/eventSourcing/assert/",
			RETRACT="http://drugis.org/eventSourcing/retract/";
	public static final Node esClassDataset = NodeFactory.createURI(ES + "EventSourcedDataset"),
			esClassLog = NodeFactory.createURI(ES + "Log"),
			esClassEvent = NodeFactory.createURI(ES + "Event"),
			esClassRevision = NodeFactory.createURI(ES + "Revision"),
			esPropertyHead = NodeFactory.createURI(ES + "head"),
			esPropertyGraph = NodeFactory.createURI(ES + "graph"),
			esPropertyRevision = NodeFactory.createURI(ES + "has_revision"),
			esPropertyAssertions = NodeFactory.createURI(ES + "assertions"),
			esPropertyRetractions = NodeFactory.createURI(ES + "retractions"),
			dcDate = NodeFactory.createURI("http://purl.org/dc/elements/1.1/date"),
			dcCreator = NodeFactory.createURI("http://purl.org/dc/elements/1.1/creator");

	
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
		return getUniqueOptionalObject(eventSource.find(log, head, RDF.Nodes.first, Node.ANY));
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
	
	private static Node getUniqueOptionalObject(Iterator<Quad> result) {
		if (result.hasNext()) {
			Node object = result.next().getObject();
			if (result.hasNext()) {
				throw new IllegalStateException("Multiple subjects on property of arity 1");
			}
			return object;
		}
		return null;
	}
	
	private static Node getUniqueObject(Iterator<Quad> result) {
		Node object = getUniqueOptionalObject(result);
		if (object == null) {
			throw new IllegalStateException("Zero subjects on property of arity 1");
		}
		return object;
	}

	public static DatasetGraph applyEvent(DatasetGraph eventSource, Node log, DatasetGraph base, Node event) {
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

    // http://stackoverflow.com/questions/3914404
	private static String now() {
	    TimeZone tz = TimeZone.getTimeZone("UTC");
	    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	    df.setTimeZone(tz);
	    String nowAsISO = df.format(new Date());
	    return nowAsISO;
	}
	
	/**
	 * Write an event to the log, assuming it is consistent with the current state.
	 * @param event The event (changeset).
	 * @return The ID of the event.
	 */
	public static Node writeToLog(DatasetGraph eventSource, Node log, DatasetGraphDelta event) {
		return writeToLog(eventSource, log, event, GraphFactory.createGraphMem());
	}

	/**
	 * Write an event to the log, assuming it is consistent with the current state.
	 * @param eventSource The DatasetGraph containing the event log.
	 * @param log The URI of the event log.
	 * @param event The event (changeset).
	 * @param meta A graph containing meta-data. It must contain a single blank node of class es:Event, the properties of which will be added to the event meta-data.
	 * @return The ID of the event.
	 */
	public static Node writeToLog(DatasetGraph eventSource, Node log, DatasetGraphDelta event, Graph meta) {
		Node eventId = NodeFactory.createURI(EVENT + UUID.randomUUID().toString());
		
		eventSource.add(log, eventId, RDF.Nodes.type, esClassEvent);
		eventSource.add(log, eventId, dcDate, NodeFactory.createLiteral(now(), XSDDatatype.XSDdateTime));
		
		// If meta-data is supplied, add it to the event log
		Set<Triple> metaRoots = meta.find(Node.ANY, RDF.Nodes.type, esClassEvent).toSet();
		if (metaRoots.size() > 1) {
			throw new IllegalStateException(
					"The supplied meta-data must have at most one resource of class "
					+ esClassEvent.getURI() + " but found " + metaRoots.size());
		} else if (metaRoots.size() == 1) {
			Node root = metaRoots.iterator().next().getSubject();
			
			// Restrict meta-data to describing the current event only
			meta = (new GraphExtract(TripleBoundary.stopNowhere)).extract(root, meta);
			
			// Prevent the setting of predicates we set ourselves
			meta.remove(root, RDF.Nodes.type, Node.ANY);
			meta.remove(root, EventSource.esPropertyRevision, Node.ANY);
			meta.remove(root, EventSource.dcDate, Node.ANY);
			
			// Replace the temporary root node by the event URI
			replaceNode(meta, root, eventId);
			
			// Copy the data into the event log
			GraphUtil.addInto(eventSource.getGraph(log), meta);
		}

		for (Node graph : event.getModifications().keySet()) {
			eventSource.add(log, eventId, esPropertyRevision, writeRevision(eventSource, log, event, graph));
		}
		
		Node oldHead = getUniqueObject(eventSource.find(log, log, esPropertyHead, Node.ANY));
		eventSource.delete(log, log, esPropertyHead, oldHead);
		Node newHead = NodeFactory.createAnon();
		eventSource.add(log, newHead, RDF.Nodes.first, eventId);
		eventSource.add(log, newHead, RDF.Nodes.rest, oldHead);
		eventSource.add(log, log, esPropertyHead, newHead);
		
		return eventId;
	}

	private static void replaceNode(Graph graph, Node oldNode, Node newNode) {
		for (Iterator<Triple> it = graph.find(oldNode, Node.ANY, Node.ANY); it.hasNext(); ) {
			Triple triple = it.next();
			graph.add(new Triple(newNode, triple.getPredicate(), triple.getObject()));
		}
		graph.remove(oldNode, Node.ANY, Node.ANY);
	}

	/**
	 * Write a revision to the log.
	 */
	private static Node writeRevision(DatasetGraph eventSource, Node log, DatasetGraphDelta event, Node graph) {
		String revId = UUID.randomUUID().toString();
		Node revisionId = NodeFactory.createURI(REVISION + revId);
		Node assertId = NodeFactory.createURI(ASSERT + revId);
		Node retractId = NodeFactory.createURI(RETRACT + revId);
		
		eventSource.add(log, revisionId, RDF.Nodes.type, esClassRevision);
		eventSource.add(log, revisionId, esPropertyGraph, graph);

		Delta delta = event.getModifications().get(graph);
		if (!delta.getAdditions().isEmpty()) {
			eventSource.add(log, revisionId, esPropertyAssertions, assertId);
			eventSource.addGraph(assertId, delta.getAdditions());
		}
		if (!delta.getDeletions().isEmpty()) {
			eventSource.add(log, revisionId, esPropertyRetractions, retractId);
			eventSource.addGraph(retractId, delta.getDeletions());
		}

		return revisionId;
	}

	public static void createLogIfNotExists(DatasetGraph eventSource, Node log) {
		Transactional trans = (Transactional) eventSource;

		trans.begin(ReadWrite.READ);
		boolean logExists = eventSource.containsGraph(log);
		trans.end();

		if (!logExists) {
			trans.begin(ReadWrite.WRITE);
			Graph graph = GraphFactory.createGraphMem();
			graph.add(new Triple(log, RDF.Nodes.type, esClassLog));
			graph.add(new Triple(log, esPropertyHead, RDF.Nodes.nil));
			eventSource.addGraph(log, graph);
			trans.commit();
		}

	}
}
