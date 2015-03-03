package org.drugis.rdf.versioning.server;

import java.util.Collections;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.sparql.core.DatasetGraph;

import es.DatasetGraphEventSourcing;
import es.EventSource;

@Controller
@RequestMapping("/datasets/{datasetId}/data")
public class GraphStoreController {
	@Autowired EventSource d_eventSource;

	@RequestMapping(method={RequestMethod.GET, RequestMethod.HEAD})
	@ResponseBody
	public Graph get(
			@PathVariable String datasetId,
			@RequestParam Map<String,String> params,
			@RequestHeader(value=ESHeaders.ACCEPT_VERSION, required=false) String version,
			HttpServletResponse response) {
		final DatasetGraphEventSourcing dataset = getDataset(datasetId);
		Node graph = NodeFactory.createURI(determineTargetGraph(params).getUri());
		dataset.begin(ReadWrite.READ);
		Graph rval;
		if (version == null) {
			rval = dataset.getGraph(graph);
			version = dataset.getLatestEvent().getURI();
		} else {
			DatasetGraph view = dataset.getView(NodeFactory.createURI(version));
			if (view == null) {
				dataset.end();
				throw new VersionNotFoundException();
			}
			rval = view.getGraph(graph);
		}
		dataset.end();
		response.setHeader(ESHeaders.VERSION, version);
		response.setHeader(HttpHeaders.VARY, HttpHeaders.ACCEPT + ", " + ESHeaders.ACCEPT_VERSION);
		return rval;
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
		final Node target = NodeFactory.createURI(determineTargetGraph(params).getUri());
		Runnable action = new Runnable() {
			@Override
			public void run() {
				dataset.addGraph(target, graph);
			}
		};
		String newVersion = Util.runReturningVersion(dataset, version, action, Util.versionMetaData(request));
		response.setHeader("X-EventSource-Version", newVersion);
	}
	
	@RequestMapping(method=RequestMethod.POST)
	public void post(
			@PathVariable String datasetId,
			@RequestParam Map<String,String> params,
			@RequestHeader(value="X-Accept-EventSource-Version", required=false) String version,
			final @RequestBody Graph graph,
			HttpServletRequest request,
			HttpServletResponse response) {
		final DatasetGraphEventSourcing dataset = getDataset(datasetId);
		final Node target = NodeFactory.createURI(determineTargetGraph(params).getUri());
		Runnable action = new Runnable() {
			@Override
			public void run() {
				GraphUtil.addInto(dataset.getGraph(target), graph);
			}
		};
		String newVersion = Util.runReturningVersion(dataset, version, action, Util.versionMetaData(request));
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
		final Node target = NodeFactory.createURI(determineTargetGraph(params).getUri());
		Runnable action = new Runnable() {
			@Override
			public void run() {
				dataset.removeGraph(target);
			}
		};
		String newVersion = Util.runReturningVersion(dataset, version, action, Util.versionMetaData(request));
		response.setHeader("X-EventSource-Version", newVersion);
	}
	
	private DatasetGraphEventSourcing getDataset(String datasetId) {
		return Util.getDataset(d_eventSource, datasetId);
	}

	static class TargetGraph {
		public String getUri() {
			return null;
		}
		public boolean isDefault() {
			return false;
		}
	}
	
	static class DefaultGraph extends TargetGraph {
		@Override
		public boolean isDefault() {
			return true;
		}
	}
	
	static class NamedGraph extends TargetGraph {
		private String d_uri;

		public NamedGraph(String uri) {
			d_uri = uri;
		}
		
		@Override
		public String getUri() {
			return d_uri;
		}
	}
	
	private static TargetGraph determineTargetGraph(Map<String, String> params) {
		if (params.keySet().equals(Collections.singleton("default")) && params.get("default").equals("")) {
			throw new NotImplementedException("Accessing default graph not (yet) implemented");
			//return new DefaultGraph();
		} else if (params.keySet().equals(Collections.singleton("graph"))) {
			return new NamedGraph(params.get("graph"));
		} else {
			throw new InvalidGraphSpecificationException();
		}
	}
}
