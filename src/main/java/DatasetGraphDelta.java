import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.compose.Delta;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.DatasetGraphBase;
import com.hp.hpl.jena.sparql.core.DatasetGraphFactory;
import com.hp.hpl.jena.sparql.core.GraphView;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.graph.GraphFactory;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;

/**
 * A read-write Dataset that tracks changes.
 */
@SuppressWarnings( "deprecation" )
public class DatasetGraphDelta extends DatasetGraphBase {
	
	private DatasetGraph d_next;
	private DatasetGraph d_base;
	private Map<Node, Delta> d_touched;

	public DatasetGraphDelta(DatasetGraph base) {
		d_base = base;
		d_next = DatasetGraphFactory.create(d_base);
		d_touched = new HashMap<Node, Delta>();
	}
	
	/**
	 * @return A map from modified graphs to a Delta recording the changes.
	 */
	public Map<Node, Delta> getModifications() {
		return Collections.unmodifiableMap(d_touched);
	}

	private Graph touchGraph(Node graphName) {
		if (!d_touched.containsKey(graphName)) {
			Graph graph = d_base.getGraph(graphName);
			if (graph == null) {
				graph = GraphFactory.createGraphMem();
			}
			Delta delta = new Delta(graph);
			d_next.addGraph(graphName, delta);
			d_touched.put(graphName, delta);
		}
		return d_touched.get(graphName);
	}

	@Override
	public Graph getDefaultGraph() {
		return GraphView.createDefaultGraph(this);
	}

	@Override
	public Graph getGraph(Node graphNode) {
		return GraphView.createNamedGraph(this, graphNode);
	}

	@Override
	public boolean containsGraph(Node graphNode) {
		return d_next.containsGraph(graphNode);
	}

	@Override
	public void setDefaultGraph(Graph g) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	public void addGraph(Node graphName, Graph graph) {
        Graph target = touchGraph(graphName);
        target.clear();
		for (ExtendedIterator<Triple> it = graph.find(Node.ANY, Node.ANY, Node.ANY); it.hasNext(); ) {
			target.add(it.next());
		}
	}

	@Override
	public void removeGraph(Node graphName) {
		touchGraph(graphName).clear();
	}

	@Override
	public Iterator<Node> listGraphNodes() {
		return d_next.listGraphNodes();
	}

	@Override
	public void add(Quad quad) {
		touchGraph(quad.getGraph()).add(quad.asTriple());
	}

	@Override
	public void delete(Quad quad) {
		touchGraph(quad.getGraph()).delete(quad.asTriple());
	}

	@Override
	public Iterator<Quad> find(Node g, Node s, Node p, Node o) {
		return d_next.find(g, s, p, o);
	}

	@Override
	public Iterator<Quad> findNG(Node g, Node s, Node p, Node o) {
		return d_next.findNG(g, s, p, o);
	}

	@Override
	public boolean contains(Node g, Node s, Node p, Node o) {
		if (d_next.containsGraph(g)) { // work-around for JENA-792
			return d_next.contains(g, s, p, o);
		} else {
			return false;
		}
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isEmpty() {
		return d_next.isEmpty();
	}

	@Override
	public long size() {
		return d_next.size();
	}
}
