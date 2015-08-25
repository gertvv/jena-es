package org.drugis.rdf.versioning.server;

import java.util.Collections;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.drugis.rdf.versioning.store.DatasetGraphEventSourcing;
import org.drugis.rdf.versioning.store.EventSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.GraphUtil;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.graph.GraphFactory;
import com.hp.hpl.jena.vocabulary.RDF;

@Controller
@RequestMapping("/datasets/{datasetId}/data")
public class GraphStoreController {
	@Autowired EventSource d_eventSource;
	Log d_log = LogFactory.getLog(getClass());

	@RequestMapping(method={RequestMethod.GET, RequestMethod.HEAD})
	@ResponseBody
	public Graph get(
			@PathVariable String datasetId,
			@RequestParam Map<String,String> params,
			@RequestHeader(value=ESHeaders.ACCEPT_VERSION, required=false) String version,
			HttpServletResponse response) {
		final DatasetGraphEventSourcing dataset = getDataset(datasetId);
		TargetGraph target = determineTargetGraph(params);

		d_log.debug("GraphStore GET " + datasetId + " " + target);

		dataset.begin(ReadWrite.READ);
		try {
			Graph rval;
			if (version == null) {
				rval = target.get(dataset);
				version = dataset.getLatestEvent().getURI();
			} else {
				DatasetGraph view = dataset.getView(NodeFactory.createURI(version));
				if (view == null) {
					throw new VersionNotFoundException();
				}
				rval = target.get(view);
			}
			response.setHeader(ESHeaders.VERSION, version);
			response.setHeader(HttpHeaders.VARY, HttpHeaders.ACCEPT + ", " + ESHeaders.ACCEPT_VERSION);
			return rval;
		} finally {
			dataset.end();
		}
	}
	
	@RequestMapping(method=RequestMethod.PUT)
	public void put(
			@PathVariable String datasetId,
			@RequestParam Map<String,String> params,
			@RequestHeader(value="X-Accept-EventSource-Version", required=false) String version,
			final @RequestBody Graph graph,
			HttpServletRequest request,
			HttpServletResponse response) {
		final DatasetGraphEventSourcing dataset = getDataset(datasetId);
		final TargetGraph target = determineTargetGraph(params);

		d_log.debug("GraphStore PUT " + datasetId + " " + target);

		Runnable action = new Runnable() {
			@Override
			public void run() {
				target.set(dataset, graph);
			}
		};
		String newVersion = Util.runReturningVersion(dataset, version, action, Util.versionMetaData(request));
		response.setHeader("X-EventSource-Version", newVersion);
	}
	
	public String handleCopyOfParam(Graph graph, Map<String, String> params) {
		System.out.println(graph);
		System.out.println(params);
		if ((graph == null && !params.containsKey("copyOf")) || (graph != null && params.containsKey("copyOf"))) {
			throw new BadRequestException("Expected one and only one of:\n - An RDF graph in the request body\n - A revision URI in the copyOf URL parameter\n");
		}
		if (params.containsKey("copyOf")) { 
			String copyOf = params.get("copyOf");
			params.remove("copyOf");
			return copyOf;
		}
		return null;
	}
	
	@RequestMapping(method=RequestMethod.POST)
	public void post(
			@PathVariable String datasetId,
			final @RequestParam Map<String,String> params,
			@RequestHeader(value="X-Accept-EventSource-Version", required=false) String version,
			final @RequestBody(required=false) Graph graph,
			HttpServletRequest request,
			HttpServletResponse response) {
		final String copyOf = handleCopyOfParam(graph, params);
		final DatasetGraphEventSourcing dataset = getDataset(datasetId);
		final TargetGraph target = determineTargetGraph(params);
		
		d_log.debug("GraphStore POST " + datasetId + " " + target + " " + (copyOf != null ? "copyOf=" + copyOf : "(request body)"));

		final Node sourceRevisionUri = copyOf != null ? NodeFactory.createURI(copyOf) : null;

		Runnable action = new Runnable() {
			@Override
			public void run() {
				if (copyOf != null) {
					target.set(dataset, d_eventSource.getRevision(sourceRevisionUri));
				} else {
					GraphUtil.addInto(target.get(dataset), graph);
				}
			}
		};

		Graph versionMetaData = Util.versionMetaData(request);
		if (copyOf != null) {
			Node graphRev = NodeFactory.createAnon();
			Node newRevision = NodeFactory.createAnon();
			if (target.isDefault()) {
				versionMetaData.add(Triple.create(graphRev, RDF.Nodes.type, EventSource.esClassDefaultGraphRevision));
			} else {
				versionMetaData.add(Triple.create(graphRev, RDF.Nodes.type, EventSource.esClassNamedGraphRevision));
				versionMetaData.add(Triple.create(graphRev, EventSource.esPropertyGraph, NodeFactory.createURI(target.getUri())));
			}
			versionMetaData.add(Triple.create(graphRev, EventSource.esPropertyRevision, newRevision));
			versionMetaData.add(Triple.create(newRevision, EventSource.esPropertyMergedRevision, sourceRevisionUri));
			versionMetaData.add(Triple.create(newRevision, EventSource.esPropertyMergeType, EventSource.esClassMergeTypeCopyTheirs));
		}

		String newVersion = Util.runReturningVersion(dataset, version, action, versionMetaData);
		response.setHeader("X-EventSource-Version", newVersion);
	}

	@RequestMapping(method=RequestMethod.DELETE)
	public void delete(
			@PathVariable String datasetId,
			@RequestParam Map<String,String> params,
			@RequestHeader(value="X-Accept-EventSource-Version", required=false) String version,
			HttpServletRequest request,
			HttpServletResponse response) {
		final DatasetGraphEventSourcing dataset = getDataset(datasetId);
		final TargetGraph target = determineTargetGraph(params);

		d_log.debug("GraphStore DELETE " + datasetId + " " + target);

		Runnable action = new Runnable() {
			@Override
			public void run() {
				target.remove(dataset);
			}
		};
		String newVersion = Util.runReturningVersion(dataset, version, action, Util.versionMetaData(request));
		response.setHeader("X-EventSource-Version", newVersion);
	}
	
	private DatasetGraphEventSourcing getDataset(String datasetId) {
		return Util.getDataset(d_eventSource, datasetId);
	}

	static abstract class TargetGraph {
		public String getUri() {
			return null;
		}

		/**
		 * Remove the target graph.
		 */
		abstract public void remove(DatasetGraphEventSourcing dataset);

		/**
		 * Get the contents of the target graph.
		 */
		abstract public Graph get(DatasetGraph dsg);

		/**
		 * Set the contents of the target graph.
		 */
		abstract public void set(DatasetGraphEventSourcing dataset, Graph graph);

		public boolean isDefault() {
			return false;
		}
	}
	
	static class DefaultGraph extends TargetGraph {
		@Override
		public boolean isDefault() {
			return true;
		}

		@Override
		public Graph get(DatasetGraph dsg) {
			return dsg.getDefaultGraph();
		}

		@Override
		public void set(DatasetGraphEventSourcing dataset, Graph graph) {
			dataset.setDefaultGraph(graph);
		}

		@Override
		public void remove(DatasetGraphEventSourcing dataset) {
			dataset.setDefaultGraph(GraphFactory.createGraphMem());
		}
		
		@Override
		public String toString() {
			return "DefaultGraph()";
		}
	}
	
	static class NamedGraph extends TargetGraph {
		private Node d_graphNode;

		public NamedGraph(String uri) {
			d_graphNode = NodeFactory.createURI(uri);
		}
		
		@Override
		public String getUri() {
			return d_graphNode.getURI();
		}

		@Override
		public Graph get(DatasetGraph dsg) {
			return dsg.getGraph(d_graphNode);
		}

		@Override
		public void set(DatasetGraphEventSourcing dataset, Graph graph) {
			dataset.addGraph(d_graphNode, graph);
		}

		@Override
		public void remove(DatasetGraphEventSourcing dataset) {
			dataset.removeGraph(d_graphNode);
		}
		
		@Override
		public String toString() {
			return "NamedGraph(" + getUri() + ")";
		}
	}
	
	private static TargetGraph determineTargetGraph(Map<String, String> params) {
		if (params.keySet().equals(Collections.singleton("default")) && params.get("default").equals("")) {
			return new DefaultGraph();
		} else if (params.keySet().equals(Collections.singleton("graph"))) {
			return new NamedGraph(params.get("graph"));
		} else {
			throw new InvalidGraphSpecificationException();
		}
	}
}
