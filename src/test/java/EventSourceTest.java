import static org.junit.Assert.*;
import static com.hp.hpl.jena.graph.NodeFactory.*;

import org.apache.jena.riot.RDFDataMgr;
import org.junit.Before;
import org.junit.Test;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.DatasetGraphFactory;
import com.hp.hpl.jena.sparql.graph.GraphFactory;
import com.hp.hpl.jena.vocabulary.RDF;


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
		assertTrue(d_dataset.contains(d_logUri, d_logUri, RDF.type.asNode(), createURI(ES + "Log")));
		assertTrue(d_dataset.contains(d_logUri, d_event1Uri, RDF.type.asNode(), createURI(ES + "Event")));
		assertTrue(d_dataset.contains(d_logUri, d_event2Uri, RDF.type.asNode(), createURI(ES + "Event")));
		assertTrue(d_dataset.contains(d_logUri, d_rev1Uri, RDF.type.asNode(), createURI(ES + "Revision")));
		assertTrue(d_dataset.contains(d_logUri, d_rev2Uri, RDF.type.asNode(), createURI(ES + "Revision")));
		assertTrue(d_dataset.contains(d_logUri, d_rev3Uri, RDF.type.asNode(), createURI(ES + "Revision")));
	}
	
	@Test
	public void testApplyRevision() {
		Graph base = GraphFactory.createGraphMem();
		Graph graph = EventSource2.applyRevision(d_dataset, d_logUri, base, d_rev1Uri);
		assertTrue(graph.contains(createURI("http://example.com/PeterParker"), RDF.type.asNode(), createURI(FOAF + "Person")));
		assertTrue(graph.contains(createURI("http://example.com/PeterParker"), createURI(FOAF + "name"), createLiteral("Spiderman")));
		assertTrue(graph.contains(createURI("http://example.com/PeterParker"), createURI(FOAF + "name"), createLiteral("Peter Parker")));
		assertEquals(3, graph.size());
		
		graph = EventSource2.applyRevision(d_dataset, d_logUri, graph, d_rev3Uri);
		assertTrue(graph.contains(createURI("http://example.com/PeterParker"), RDF.type.asNode(), createURI(FOAF + "Person")));
		assertTrue(graph.contains(createURI("http://example.com/PeterParker"), createURI(FOAF + "name"), createLiteral("Peter Parker")));
		assertTrue(graph.contains(createURI("http://example.com/PeterParker"), createURI(FOAF + "homepage"), createURI("http://www.okcupid.com/profile/PeterParker")));
		assertEquals(3, graph.size());
	}
	
	@Test
	public void testApplyEvent() {
		DatasetGraph ds = DatasetGraphFactory.createMem();
		ds = EventSource2.applyEvent(d_dataset, d_logUri, ds, d_event1Uri);
		Graph graph = ds.getGraph(createURI("http://example.com/PeterParker"));
		assertTrue(graph.contains(createURI("http://example.com/PeterParker"), RDF.type.asNode(), createURI(FOAF + "Person")));
		assertTrue(graph.contains(createURI("http://example.com/PeterParker"), createURI(FOAF + "name"), createLiteral("Spiderman")));
		assertTrue(graph.contains(createURI("http://example.com/PeterParker"), createURI(FOAF + "name"), createLiteral("Peter Parker")));
		assertEquals(3, graph.size());
		assertEquals(1, ds.size());
		
		ds = EventSource2.applyEvent(d_dataset, d_logUri, ds, d_event2Uri);
		graph = ds.getGraph(createURI("http://example.com/PeterParker"));
		assertTrue(graph.contains(createURI("http://example.com/PeterParker"), RDF.type.asNode(), createURI(FOAF + "Person")));
		assertTrue(graph.contains(createURI("http://example.com/PeterParker"), createURI(FOAF + "name"), createLiteral("Peter Parker")));
		assertTrue(graph.contains(createURI("http://example.com/PeterParker"), createURI(FOAF + "homepage"), createURI("http://www.okcupid.com/profile/PeterParker")));
		assertEquals(3, graph.size());
		graph = ds.getGraph(createURI("http://example.com/Spiderman"));
		assertTrue(graph.contains(createURI("http://example.com/Spiderman"), RDF.type.asNode(), createURI(FOAF + "Person")));
		assertTrue(graph.contains(createURI("http://example.com/Spiderman"), createURI(FOAF + "name"), createLiteral("Spiderman")));
		assertEquals(2, graph.size());
		assertEquals(2, ds.size());
	}

}
