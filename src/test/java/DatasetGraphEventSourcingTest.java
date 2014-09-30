import static com.hp.hpl.jena.graph.NodeFactory.createLiteral;
import static com.hp.hpl.jena.graph.NodeFactory.createURI;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.DatasetGraphFactory;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.graph.GraphFactory;


public class DatasetGraphEventSourcingTest {

	private DatasetGraph d_base;
	private DatasetGraphDelta d_next;
	
	private Node d_graphA;
	private Node d_graphB;
	private Node d_graphC;
	private Quad d_A1has2;
	private Quad d_A1name;
	private Quad d_B2name;

	@Before
	public void setUp() throws Exception {
		d_base = DatasetGraphFactory.createMem();
		
		d_graphA = createURI("http://example.com/GraphA");
		d_graphB = createURI("http://example.com/GraphB");
		d_graphC = createURI("http://example.com/GraphC");
		
		d_A1has2 = new Quad(
				d_graphA,
				createURI("http://example.com/Thing1"),
				createURI("http://example.com/has"),
				createURI("http://example.com/Thing2"));
		d_base.add(d_A1has2);

		d_A1name = new Quad(
				d_graphA,
				createURI("http://example.com/Thing1"),
				createURI("http://example.com/name"),
				createLiteral("Thing 1"));
		d_base.add(d_A1name);

		d_B2name = new Quad(
				d_graphB,
				createURI("http://example.com/Thing2"),
				createURI("http://example.com/name"),
				createLiteral("Thing 2"));
		d_base.add(d_B2name);
		
		d_next = new DatasetGraphDelta(d_base);
	}

	@Test
	public void testReadAccess() {
		assertTrue(d_next.contains(d_A1has2));
		assertTrue(d_next.contains(d_A1name));
		assertTrue(d_next.contains(d_B2name));
		assertTrue(d_next.containsGraph(d_graphA));
		assertTrue(d_next.containsGraph(d_graphB));
		assertEquals(2, d_next.size());
		assertEquals(2, d_next.getGraph(d_graphA).size());
		assertEquals(1, d_next.getGraph(d_graphB).size());
	}
	
	@Test
	public void testRemoveGraph() {
		d_next.removeGraph(d_graphA);
		assertFalse(d_next.contains(d_A1has2));
		assertTrue(d_base.contains(d_A1has2));
		assertFalse(d_next.contains(d_A1name));
		assertTrue(d_base.contains(d_A1name));
		assertTrue(d_next.contains(d_B2name));
		assertTrue(d_base.contains(d_B2name));
	}
	
	@Test
	public void testAddGraph() {
		Graph graph = GraphFactory.createGraphMem();
        Quad quad = new Quad(d_graphC, createURI("http://example.com/Thing3"), createURI("http://example.com/name"), createLiteral("Thing 3"));
		graph.add(quad.asTriple());
		d_next.addGraph(d_graphC, graph);
		assertTrue(d_next.containsGraph(d_graphC));
		assertTrue(d_next.contains(quad));
		assertFalse(d_base.contains(quad));
	}
	
	@Test
	public void addToExistingGraph() {
		Quad quad1 = new Quad(d_graphB, createURI("http://example.com/Thing3"), createURI("http://example.com/name"), createLiteral("Thing 3"));
		d_next.add(quad1);
		assertTrue(d_next.contains(quad1));
		assertTrue(d_next.contains(d_B2name));
		assertFalse(d_base.contains(quad1));
		assertTrue(d_base.contains(d_B2name));
		Quad quad2 = new Quad(d_graphB, createURI("http://example.com/Thing3"), createURI("http://example.com/name"), createLiteral("Thing 3b"));
		d_next.add(quad2);
		assertTrue(d_next.contains(quad2));
		assertTrue(d_next.contains(quad1));
		assertTrue(d_next.contains(d_B2name));
		assertFalse(d_base.contains(quad2));
	}
	
	@Test
	public void addToNewGraph() {
		Quad quad1 = new Quad(d_graphC, createURI("http://example.com/Thing3"), createURI("http://example.com/name"), createLiteral("Thing 3"));
		d_next.add(quad1);
		assertTrue(d_next.contains(quad1));
		assertFalse(d_base.contains(quad1));
		Quad quad2 = new Quad(d_graphC, createURI("http://example.com/Thing3"), createURI("http://example.com/name"), createLiteral("Thing 3b"));
		d_next.add(quad2);
		assertTrue(d_next.contains(quad2));
		assertTrue(d_next.contains(quad1));
		assertFalse(d_base.contains(quad2));
	}
	
	@Test
	public void deleteFromExistingGraph() {
		d_next.delete(d_A1has2);
		assertTrue(d_next.contains(d_A1name));
		assertTrue(d_next.contains(d_B2name));
		assertFalse(d_next.contains(d_A1has2));
		d_next.delete(d_A1name);
		assertFalse(d_next.contains(d_A1name));

		assertTrue(d_base.contains(d_A1has2));
		assertTrue(d_base.contains(d_A1name));
		assertTrue(d_base.contains(d_B2name));
	}
	
	@Test
	public void deleteNonExistent() {
		Quad quad1 = new Quad(d_graphC, createURI("http://example.com/Thing3"), createURI("http://example.com/name"), createLiteral("Thing 3"));
		assertFalse(d_next.contains(quad1));
		d_next.delete(quad1);
		assertFalse(d_next.contains(quad1));
	}
	
	@Test
	public void addThenDelete() {
		Quad quad1 = new Quad(d_graphC, createURI("http://example.com/Thing3"), createURI("http://example.com/name"), createLiteral("Thing 3"));
		d_next.add(quad1);
		d_next.delete(quad1);
		assertFalse(d_next.contains(quad1));
	}
	
	@Ignore
	@Test public void test() { // Test for JENA-792
		DatasetGraph mem = DatasetGraphFactory.createMem();
		DatasetGraph ds = DatasetGraphFactory.create(mem);
		ds.contains(NodeFactory.createURI("http://example.com/DoesNotExist"),
				NodeFactory.createURI("http://example.com/subject"),
				NodeFactory.createURI("http://example.com/predicate"),
				NodeFactory.createURI("http://example.com/object"));
	}

}
