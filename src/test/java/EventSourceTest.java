import static com.hp.hpl.jena.graph.NodeFactory.createAnon;
import static com.hp.hpl.jena.graph.NodeFactory.createLiteral;
import static com.hp.hpl.jena.graph.NodeFactory.createURI;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.drugis.rdf.versioning.store.DatasetGraphDelta;
import org.drugis.rdf.versioning.store.DatasetGraphEventSourcing;
import org.drugis.rdf.versioning.store.EventSource;
import org.junit.Before;
import org.junit.Test;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.DatasetGraphWithLock;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.graph.GraphFactory;
import com.hp.hpl.jena.util.IteratorCollection;
import com.hp.hpl.jena.vocabulary.RDF;


public class EventSourceTest {

	private static final String ES="http://drugis.org/eventSourcing/es#",
			DATASET="http://example.com/datasets/",
			VERSION="http://example.com/versions/",
			REVISION="http://example.com/revisions/",
			FOAF="http://xmlns.com/foaf/0.1/",
			ID_GOBLIN_DATASET = "ubb245f8sz",
			ID_SPIDER_DATASET = "qmk2x16nz1",
			ID_GOBLIN_VERSION0 = "3ucq3j5c7u",
			ID_GOBLIN_VERSION1 = "7wi4xglx1c",
			ID_SPIDER_VERSION0 = "f98gj2sgsn",
			ID_SPIDER_VERSION1 = "g05ri5qvvq",
			ID_REV1 = "38fc1de7a-43ea-11e4-a12c-3314171ce0bb",
			ID_REV2 = "302431f4-43e8-11e4-8745-c72e64fa66b1",
			ID_REV3 = "44ea0618-43e8-11e4-bcfb-bba47531d497";

	private static final Node FOAF_PERSON = createURI(FOAF + "Person");
	private static final Node FOAF_NAME = createURI(FOAF + "name");
	private static final Node FOAF_KNOWS = createURI(FOAF + "knows");
	private static final Node SPIDERMAN = createURI("http://example.com/Spiderman");
	private static final Node PETER_PARKER = createURI("http://example.com/PeterParker");

	private DatasetGraph d_datastore;
	private EventSource d_eventSource;

	private Node d_goblinDatasetUri;
	private Node d_spiderDatasetUri;
	private Node d_goblinV0Uri;
	private Node d_goblinV1Uri;
	private Node d_spiderV0Uri;
	private Node d_spiderV1Uri;
	private Node d_rev1Uri;
	private Node d_rev2Uri;
	private Node d_rev3Uri;


	@Before
	public void setUp() throws Exception {
		d_datastore = RDFDataMgr.loadDataset("data.trig").asDatasetGraph();
		d_eventSource = new EventSource(d_datastore, "http://example.com/");
		d_goblinDatasetUri = createURI(DATASET + ID_GOBLIN_DATASET);
		d_spiderDatasetUri = createURI(DATASET + ID_SPIDER_DATASET);
		d_goblinV0Uri = createURI(VERSION + ID_GOBLIN_VERSION0);
		d_goblinV1Uri = createURI(VERSION + ID_GOBLIN_VERSION1);
		d_spiderV0Uri = createURI(VERSION + ID_SPIDER_VERSION0);
		d_spiderV1Uri = createURI(VERSION + ID_SPIDER_VERSION1);
		d_rev1Uri = createURI(REVISION + ID_REV1);
		d_rev2Uri = createURI(REVISION + ID_REV2);
		d_rev3Uri = createURI(REVISION + ID_REV3);
	}

	@Test
	public void test() {
		assertTrue(d_datastore.getDefaultGraph().contains(d_goblinDatasetUri, RDF.Nodes.type, createURI(ES + "Dataset")));
		assertTrue(d_datastore.getDefaultGraph().contains(d_spiderDatasetUri, RDF.Nodes.type, createURI(ES + "Dataset")));
		assertTrue(d_datastore.getDefaultGraph().contains(d_goblinV0Uri, RDF.Nodes.type, createURI(ES + "DatasetVersion")));
		assertTrue(d_datastore.getDefaultGraph().contains(d_goblinV1Uri, RDF.Nodes.type, createURI(ES + "DatasetVersion")));
		assertTrue(d_datastore.getDefaultGraph().contains(d_spiderV0Uri, RDF.Nodes.type, createURI(ES + "DatasetVersion")));
		assertTrue(d_datastore.getDefaultGraph().contains(d_spiderV1Uri, RDF.Nodes.type, createURI(ES + "DatasetVersion")));
		assertTrue(d_datastore.getDefaultGraph().contains(d_rev1Uri, RDF.Nodes.type, createURI(ES + "Revision")));
		assertTrue(d_datastore.getDefaultGraph().contains(d_rev2Uri, RDF.Nodes.type, createURI(ES + "Revision")));
		assertTrue(d_datastore.getDefaultGraph().contains(d_rev3Uri, RDF.Nodes.type, createURI(ES + "Revision")));
	}
	
	@Test
	public void testApplyRevision() {
		Graph base = GraphFactory.createGraphMem();
		Graph graph = EventSource.applyRevision(d_datastore, base, d_rev1Uri);
		checkGraphRev1(graph);
		
		graph = EventSource.applyRevision(d_datastore, graph, d_rev3Uri);
		checkGraphRev3(graph);
	}
	
	@Test
	public void testGetRevision() {
		checkGraphRev1(d_eventSource.getRevision(d_rev1Uri));
		checkGraphRev3(d_eventSource.getRevision(d_rev3Uri));
	}
	
	@Test
	public void testGetVersion() {
		DatasetGraph ds;
		ds = d_eventSource.getVersion(d_goblinDatasetUri, d_goblinV0Uri);
		checkDatasetEmpty(ds);
		
		ds = d_eventSource.getVersion(d_goblinDatasetUri, d_goblinV1Uri);
		checkDatasetAfterEvent1(ds);
		
		ds = d_eventSource.getVersion(d_spiderDatasetUri, d_spiderV0Uri);
		checkDatasetAfterEvent1(ds);
		
		ds = d_eventSource.getVersion(d_spiderDatasetUri, d_spiderV1Uri);
		checkDatasetAfterEvent2(ds);
	}
	
	public void testGetInvalidVersion() {
		assertTrue(d_eventSource.getVersion(d_goblinDatasetUri, d_spiderV0Uri) == null);
	}

	@Test
	public void testGetLatestEventURI() {
		assertEquals(d_goblinV1Uri, d_eventSource.getLatestVersionUri(d_goblinDatasetUri));
		assertEquals(d_spiderV1Uri, d_eventSource.getLatestVersionUri(d_spiderDatasetUri));
	}
	
	@Test
	public void testWriteToLog() {
		DatasetGraph ds = d_eventSource.getLatestVersion(d_spiderDatasetUri);
		DatasetGraphDelta delta = new DatasetGraphDelta(ds);
		applyGraphMod(delta);
		d_eventSource.writeToLog(d_spiderDatasetUri, delta);
		
		ds = d_eventSource.getLatestVersion(d_spiderDatasetUri);
		RDFDataMgr.write(System.out, d_datastore.getDefaultGraph(), Lang.TURTLE);
		checkGraphAfterMod(ds);
	}
	
	@Test
	public void testWriteToLogWithMetaData() {
		DatasetGraph ds = d_eventSource.getLatestVersion(d_spiderDatasetUri);
		DatasetGraphDelta delta = new DatasetGraphDelta(ds);
		applyGraphMod(delta);
		Graph meta = GraphFactory.createGraphMem();
		Node root = createAnon();
		meta.add(new Triple(root, RDF.Nodes.type, EventSource.esClassDatasetVersion));
		meta.add(new Triple(root, EventSource.dctermsCreator, PETER_PARKER));
		Node version = d_eventSource.writeToLog(d_spiderDatasetUri, delta, meta);

		ds = d_eventSource.getLatestVersion(d_spiderDatasetUri);
		checkGraphAfterMod(ds);
		assertTrue(d_datastore.getDefaultGraph().contains(version, EventSource.dctermsCreator, PETER_PARKER));
	}

	@Test
	public void testTransactionInterface() {
		// We assume that the DSG implements Transactional, so fake it
		DatasetGraph dataset = new DatasetGraphWithLock(d_datastore) {
			@Override
			protected void _abort() {
				_end();
			}
		};
		DatasetGraphEventSourcing ds = new DatasetGraphEventSourcing(new EventSource(dataset, "http://example.com/"), d_spiderDatasetUri);

		ds.begin(ReadWrite.READ);
		checkDatasetAfterEvent2(ds);
		ds.end();

		ds.begin(ReadWrite.WRITE);
		applyGraphMod(ds);
		checkGraphAfterMod(ds);
		ds.abort();

		ds.begin(ReadWrite.READ);
		checkDatasetAfterEvent2(ds);
		ds.end();
		
		ds.begin(ReadWrite.WRITE);
		applyGraphMod(ds);
		checkGraphAfterMod(ds);
		ds.commit();

		ds.begin(ReadWrite.READ);
		checkGraphAfterMod(ds);
		ds.end();
	}
	
	@Test public void testSkolemization() {
		DatasetGraph ds = d_eventSource.getLatestVersion(d_spiderDatasetUri);
		DatasetGraphDelta delta = new DatasetGraphDelta(ds);
		Node spidermanAnon = NodeFactory.createAnon();
		delta.add(PETER_PARKER, PETER_PARKER, FOAF_KNOWS, spidermanAnon);
		Node spidermanName = NodeFactory.createLiteral("Spiderman");
		delta.add(PETER_PARKER, spidermanAnon, FOAF_NAME, spidermanName);
		Node goblinAnon = NodeFactory.createAnon();
		delta.add(PETER_PARKER, PETER_PARKER, FOAF_KNOWS, goblinAnon);

		d_eventSource.writeToLog(d_spiderDatasetUri, delta);
		
		ds = d_eventSource.getLatestVersion(d_spiderDatasetUri);
		List<Quad> quads = IteratorCollection.iteratorToList(ds.find(PETER_PARKER, PETER_PARKER, FOAF_KNOWS, Node.ANY));
		assertEquals(2, quads.size());
		assertFalse(quads.get(0).getObject().isBlank());
		assertFalse(quads.get(1).getObject().isBlank());
		assertFalse(quads.get(0).getObject().equals(quads.get(1).getObject()));
		
		List<Quad> quads2 = IteratorCollection.iteratorToList(ds.find(PETER_PARKER, Node.ANY, FOAF_NAME, spidermanName));
		assertEquals(1, quads2.size());
		assertFalse(quads2.get(0).getSubject().isBlank());
		
		assertTrue(quads.get(0).getObject().equals(quads2.get(0).getSubject()) || quads.get(1).getObject().equals(quads2.get(0).getSubject()));
	}

	private void checkDatasetEmpty(DatasetGraph ds) {
		assertEquals(0, ds.size());
	}

	private void checkGraphRev1(Graph graph) {
		assertTrue(graph.contains(PETER_PARKER, RDF.Nodes.type, FOAF_PERSON));
		assertTrue(graph.contains(PETER_PARKER, FOAF_NAME, createLiteral("Spiderman")));
		assertTrue(graph.contains(PETER_PARKER, FOAF_NAME, createLiteral("Peter Parker")));
		assertEquals(3, graph.size());
	}

	private void checkGraphRev3(Graph graph) {
		assertTrue(graph.contains(PETER_PARKER, RDF.Nodes.type, FOAF_PERSON));
		assertTrue(graph.contains(PETER_PARKER, FOAF_NAME, createLiteral("Peter Parker")));
		assertTrue(graph.contains(PETER_PARKER, createURI(FOAF + "homepage"), createURI("http://www.okcupid.com/profile/PeterParker")));
		assertEquals(3, graph.size());
	}

	private void checkDatasetAfterEvent1(DatasetGraph ds) {
		Graph graph = ds.getGraph(PETER_PARKER);
		checkGraphRev1(graph);
		assertEquals(1, ds.size());
	}

	private void checkDatasetAfterEvent2(DatasetGraph ds) {
		Graph graph = ds.getGraph(PETER_PARKER);
		checkGraphRev3(graph);
		graph = ds.getGraph(SPIDERMAN);
		assertTrue(graph.contains(SPIDERMAN, RDF.Nodes.type, FOAF_PERSON));
		assertTrue(graph.contains(SPIDERMAN, FOAF_NAME, createLiteral("Spiderman")));
		assertEquals(2, graph.size());
		assertEquals(2, ds.size());
	}

	private void applyGraphMod(DatasetGraph delta) {
		delta.add(PETER_PARKER, PETER_PARKER, createURI(FOAF + "givenName"), createLiteral("Peter"));
		delta.removeGraph(SPIDERMAN);
	}

	private void checkGraphAfterMod(DatasetGraph ds) {
		Graph graph = ds.getGraph(PETER_PARKER);
		assertTrue(graph.contains(PETER_PARKER, RDF.Nodes.type, FOAF_PERSON));
		assertTrue(graph.contains(PETER_PARKER, FOAF_NAME, createLiteral("Peter Parker")));
		assertTrue(graph.contains(PETER_PARKER, createURI(FOAF + "givenName"), createLiteral("Peter")));
		assertTrue(graph.contains(PETER_PARKER, createURI(FOAF + "homepage"), createURI("http://www.okcupid.com/profile/PeterParker")));
		assertEquals(4, graph.size());
		graph = ds.getGraph(SPIDERMAN);
		assertTrue(graph == null || graph.isEmpty());
	}
}
