import static com.hp.hpl.jena.graph.NodeFactory.createLiteral;
import static com.hp.hpl.jena.graph.NodeFactory.createURI;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.jena.riot.RDFDataMgr;
import org.junit.Before;
import org.junit.Test;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.DatasetGraphFactory;
import com.hp.hpl.jena.sparql.graph.GraphFactory;
import com.hp.hpl.jena.vocabulary.RDF;

import es.DatasetGraphDelta;
import es.DatasetGraphEventSourcing;
import es.EventSource;


public class EventSourceTest {
	private static final String ES="http://drugis.org/eventSourcing/es#",
			EVENT="http://drugis.org/eventSourcing/event/",
			REVISION="http://drugis.org/eventSourcing/revision/",
			FOAF="http://xmlns.com/foaf/0.1/",
			ID_EVENT2 = "94b23234-43e7-11e4-ac24-337dc3ef5145",
			ID_EVENT1 = "67ed1d1a-43ea-11e4-9186-776438a7b3d0",
			ID_REV1 = "38fc1de7a-43ea-11e4-a12c-3314171ce0bb",
			ID_REV2 = "302431f4-43e8-11e4-8745-c72e64fa66b1",
			ID_REV3 = "44ea0618-43e8-11e4-bcfb-bba47531d497";

	private DatasetGraph d_dataset;

	private Node d_logUri;
	private Node d_event1Uri;
	private Node d_event2Uri;
	private Node d_rev1Uri;
	private Node d_rev2Uri;
	private Node d_rev3Uri;

	@Before
	public void setUp() throws Exception {
		d_dataset = RDFDataMgr.loadDataset("data.trig").asDatasetGraph();
		d_logUri = createURI(ES + "log");
		d_event1Uri = createURI(EVENT + ID_EVENT1);
		d_event2Uri = createURI(EVENT + ID_EVENT2);
		d_rev1Uri = createURI(REVISION + ID_REV1);
		d_rev2Uri = createURI(REVISION + ID_REV2);
		d_rev3Uri = createURI(REVISION + ID_REV3);
	}

	@Test
	public void test() {
		assertTrue(d_dataset.containsGraph(d_logUri));
		assertTrue(d_dataset.contains(d_logUri, d_logUri, RDF.Nodes.type, createURI(ES + "Log")));
		assertTrue(d_dataset.contains(d_logUri, d_event1Uri, RDF.Nodes.type, createURI(ES + "Event")));
		assertTrue(d_dataset.contains(d_logUri, d_event2Uri, RDF.Nodes.type, createURI(ES + "Event")));
		assertTrue(d_dataset.contains(d_logUri, d_rev1Uri, RDF.Nodes.type, createURI(ES + "Revision")));
		assertTrue(d_dataset.contains(d_logUri, d_rev2Uri, RDF.Nodes.type, createURI(ES + "Revision")));
		assertTrue(d_dataset.contains(d_logUri, d_rev3Uri, RDF.Nodes.type, createURI(ES + "Revision")));
	}
	
	@Test
	public void testApplyRevision() {
		Graph base = GraphFactory.createGraphMem();
		Graph graph = EventSource.applyRevision(d_dataset, d_logUri, base, d_rev1Uri);
		assertTrue(graph.contains(createURI("http://example.com/PeterParker"), RDF.Nodes.type, createURI(FOAF + "Person")));
		assertTrue(graph.contains(createURI("http://example.com/PeterParker"), createURI(FOAF + "name"), createLiteral("Spiderman")));
		assertTrue(graph.contains(createURI("http://example.com/PeterParker"), createURI(FOAF + "name"), createLiteral("Peter Parker")));
		assertEquals(3, graph.size());
		
		graph = EventSource.applyRevision(d_dataset, d_logUri, graph, d_rev3Uri);
		assertTrue(graph.contains(createURI("http://example.com/PeterParker"), RDF.Nodes.type, createURI(FOAF + "Person")));
		assertTrue(graph.contains(createURI("http://example.com/PeterParker"), createURI(FOAF + "name"), createLiteral("Peter Parker")));
		assertTrue(graph.contains(createURI("http://example.com/PeterParker"), createURI(FOAF + "homepage"), createURI("http://www.okcupid.com/profile/PeterParker")));
		assertEquals(3, graph.size());
	}
	
	@Test
	public void testApplyEvent() {
		DatasetGraph ds = DatasetGraphFactory.createMem();
		ds = EventSource.applyEvent(d_dataset, d_logUri, ds, d_event1Uri);
		checkGraphAfterEvent1(ds);
		
		ds = EventSource.applyEvent(d_dataset, d_logUri, ds, d_event2Uri);
		checkGraphAfterEvent2(ds);
	}

	@Test
	public void testGetLatestEventURI() {
		assertEquals(d_event2Uri, EventSource.getLatestEvent(d_dataset, d_logUri));
	}
	
	@Test
	public void testGetEventsUntil() {
		assertEquals(Arrays.asList(new Node[] { d_event2Uri, d_event1Uri}),
				EventSource.getEventsUntil(d_dataset, d_logUri, d_event2Uri));
		assertEquals(Arrays.asList(new Node[] { d_event1Uri}),
				EventSource.getEventsUntil(d_dataset, d_logUri, d_event1Uri));
	}
	
	@Test
	public void testReplayLog() {
		DatasetGraph ds = DatasetGraphFactory.createMem();
		ds = EventSource.replayLogUntil(d_dataset, d_logUri, d_event1Uri);
		checkGraphAfterEvent1(ds);
		
		ds = EventSource.replayLogUntil(d_dataset, d_logUri, d_event2Uri);
		checkGraphAfterEvent2(ds);
	}
	
	@Test
	public void testWriteToLog() {
		DatasetGraph ds = EventSource.replayLog(d_dataset, d_logUri);
		DatasetGraphDelta delta = new DatasetGraphDelta(ds);
		applyGraphMod(delta);
		EventSource.writeToLog(d_dataset, d_logUri, delta);
		
		ds = EventSource.replayLog(d_dataset, d_logUri);
		checkGraphAfterMod(ds);
	}
	
	@Test
	public void testTransactionInterface() {
		DatasetGraphEventSourcing ds = new DatasetGraphEventSourcing(d_dataset, d_logUri);

		ds.begin(ReadWrite.READ);
		checkGraphAfterEvent2(ds);
		ds.end();

		ds.begin(ReadWrite.WRITE);
		applyGraphMod(ds);
		checkGraphAfterMod(ds);
		ds.abort();

		ds.begin(ReadWrite.READ);
		checkGraphAfterEvent2(ds);
		ds.end();
		
		ds.begin(ReadWrite.WRITE);
		applyGraphMod(ds);
		checkGraphAfterMod(ds);
		ds.commit();

		ds.begin(ReadWrite.READ);
		checkGraphAfterMod(ds);
		ds.end();
	}

	private void checkGraphAfterEvent1(DatasetGraph ds) {
		Graph graph = ds.getGraph(createURI("http://example.com/PeterParker"));
		assertTrue(graph.contains(createURI("http://example.com/PeterParker"), RDF.Nodes.type, createURI(FOAF + "Person")));
		assertTrue(graph.contains(createURI("http://example.com/PeterParker"), createURI(FOAF + "name"), createLiteral("Spiderman")));
		assertTrue(graph.contains(createURI("http://example.com/PeterParker"), createURI(FOAF + "name"), createLiteral("Peter Parker")));
		assertEquals(3, graph.size());
		assertEquals(1, ds.size());
	}

	private void checkGraphAfterEvent2(DatasetGraph ds) {
		Graph graph = ds.getGraph(createURI("http://example.com/PeterParker"));
		assertTrue(graph.contains(createURI("http://example.com/PeterParker"), RDF.Nodes.type, createURI(FOAF + "Person")));
		assertTrue(graph.contains(createURI("http://example.com/PeterParker"), createURI(FOAF + "name"), createLiteral("Peter Parker")));
		assertTrue(graph.contains(createURI("http://example.com/PeterParker"), createURI(FOAF + "homepage"), createURI("http://www.okcupid.com/profile/PeterParker")));
		assertEquals(3, graph.size());
		graph = ds.getGraph(createURI("http://example.com/Spiderman"));
		assertTrue(graph.contains(createURI("http://example.com/Spiderman"), RDF.Nodes.type, createURI(FOAF + "Person")));
		assertTrue(graph.contains(createURI("http://example.com/Spiderman"), createURI(FOAF + "name"), createLiteral("Spiderman")));
		assertEquals(2, graph.size());
		assertEquals(2, ds.size());
	}

	private void applyGraphMod(DatasetGraph delta) {
		delta.add(createURI("http://example.com/PeterParker"), createURI("http://example.com/PeterParker"), createURI(FOAF + "givenName"), createLiteral("Peter"));
		delta.removeGraph(createURI("http://example.com/Spiderman"));
	}

	private void checkGraphAfterMod(DatasetGraph ds) {
		Graph graph = ds.getGraph(createURI("http://example.com/PeterParker"));
		assertTrue(graph.contains(createURI("http://example.com/PeterParker"), RDF.Nodes.type, createURI(FOAF + "Person")));
		assertTrue(graph.contains(createURI("http://example.com/PeterParker"), createURI(FOAF + "name"), createLiteral("Peter Parker")));
		assertTrue(graph.contains(createURI("http://example.com/PeterParker"), createURI(FOAF + "givenName"), createLiteral("Peter")));
		assertTrue(graph.contains(createURI("http://example.com/PeterParker"), createURI(FOAF + "homepage"), createURI("http://www.okcupid.com/profile/PeterParker")));
		assertEquals(4, graph.size());
		graph = ds.getGraph(createURI("http://example.com/Spiderman"));
		assertTrue(graph == null || graph.isEmpty());
	}
}
