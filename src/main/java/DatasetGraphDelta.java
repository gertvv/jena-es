import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.jena.atlas.iterator.Iter;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.compose.Delta;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.shared.Lock;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.DatasetGraphBase;
import com.hp.hpl.jena.sparql.core.DatasetGraphFactory;
import com.hp.hpl.jena.sparql.core.GraphView;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.graph.GraphFactory;
import com.hp.hpl.jena.sparql.util.Context;
import com.hp.hpl.jena.update.GraphStore;


public class DatasetGraphDelta extends DatasetGraphBase implements GraphStore {
	
	private DatasetGraph d_next;
	private DatasetGraph d_current;
	private Map<Node, Graph> d_touched;

	public DatasetGraphDelta(DatasetGraph base) {
		d_current = base;
		d_next = DatasetGraphFactory.create(d_current);
		d_touched = new HashMap<Node, Graph>();
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
		d_next.addGraph(graphName, graph);
	}

	@Override
	public void removeGraph(Node graphName) {
		d_next.removeGraph(graphName);
	}

	@Override
	public Iterator<Node> listGraphNodes() {
		return d_next.listGraphNodes();
	}

	@Override
	public void add(Quad quad) {
		Graph graph = d_next.getGraph(quad.getGraph());
		if (graph == null) {
			graph = GraphFactory.createGraphMem();
		}
        graph = new Delta(graph);

        graph.add(quad.asTriple());
        d_next.addGraph(quad.getGraph(), graph);
	}

	@Override
	public void delete(Quad quad) {
		if (d_next.containsGraph(quad.getGraph())) {
			Graph graph = d_next.getGraph(quad.getGraph());
			graph = new Delta(graph);
			graph.delete(quad.asTriple());
			d_next.addGraph(quad.getGraph(), graph);
		}
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
	public Lock getLock() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Context getContext() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long size() {
		return d_next.size();
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

	@Override
	public Dataset toDataset() {
		return DatasetFactory.create(this);
	}

	@Override
	public void startRequest() {
		// NI
	}

	@Override
	public void finishRequest() {
		// NI
	}

}
