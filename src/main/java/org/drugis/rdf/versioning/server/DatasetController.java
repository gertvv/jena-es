package org.drugis.rdf.versioning.server;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.drugis.rdf.versioning.store.EventSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.GraphUtil;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.sparql.core.Transactional;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.hp.hpl.jena.vocabulary.RDF;

@Controller
@RequestMapping("/datasets")
public class DatasetController {
	@Autowired EventSource eventSource;
	@Autowired String datasetInfoQuery;
	@Autowired String currentMergedRevisionsQuery;
	@Autowired String datasetHistoryQuery;
	@Autowired String allMergedRevisionsQuery;
	Log d_log = LogFactory.getLog(getClass());

	@RequestMapping(value="", method=RequestMethod.GET, produces="text/html")
	@ResponseBody
	public String list() {
		d_log.debug("Dataset LIST");

		StringBuilder builder = new StringBuilder();
		builder.append("<html><body><ul>");
		Transactional transactional = (Transactional)eventSource.getDataStore();
		transactional.begin(ReadWrite.READ);
		try {
			ExtendedIterator<Triple> find = eventSource.getDataStore().getDefaultGraph().find(Node.ANY, RDF.Nodes.type, EventSource.esClassDataset);
			while (find.hasNext()) {
				Triple triple = find.next();
				builder.append("<li><a href=\"");
				builder.append(triple.getSubject().getURI());
				builder.append("\">");
				builder.append(triple.getSubject().getURI());
				builder.append("</a></li>");
			}
			builder.append("</ul></body></html>");
		} finally {
			transactional.end();
		}
		return builder.toString();
	}

	@RequestMapping(value="", method=RequestMethod.GET, produces="application/json")
	@ResponseBody
	public List<DatasetInfo> listAsJson() {
		d_log.debug("Dataset LIST");
		List<DatasetInfo> jsonResponse = new ArrayList<>();
		Transactional transactional = (Transactional)eventSource.getDataStore();
		transactional.begin(ReadWrite.READ);
		try {
			Graph graph = eventSource.getDataStore().getDefaultGraph();
			ExtendedIterator<Triple> find = graph.find(Node.ANY, RDF.Nodes.type, EventSource.esClassDataset);
			while (find.hasNext()) {
				Triple triple = find.next();
				Node dataset = triple.getSubject();
				Node head = Util.getUniqueOptionalObject(graph.find(dataset, EventSource.esPropertyHead, Node.ANY));
				Node creator = Util.getUniqueOptionalObject(graph.find(dataset, EventSource.dctermsCreator, Node.ANY));
				jsonResponse.add(new DatasetInfo(dataset.getURI(), head != null ? head.getURI() : null, creator != null ? creator.getURI() : null));
			}
		} finally {
			transactional.end();
		}
		return jsonResponse;
	}

	@RequestMapping(value="", method=RequestMethod.POST)
	@ResponseStatus(HttpStatus.CREATED)
	public void create(HttpServletRequest request, HttpServletResponse response, @RequestBody(required=false) Graph graph) {
		d_log.debug("Dataset CREATE");

		String id = UUID.randomUUID().toString();
		Graph meta = Util.versionMetaData(request);
		Node dataset = NodeFactory.createURI(eventSource.getDatasetUri(id));
		Node version = eventSource.createDataset(dataset, graph, meta);
		response.setHeader(HttpHeaders.LOCATION, dataset.getURI());
		response.setHeader(ESHeaders.VERSION, version.getURI());
	}

	@RequestMapping(value="/{id}", method=RequestMethod.GET)
	@ResponseBody
	public Graph get(@PathVariable String id) {
		d_log.debug("Dataset GET " + id);
		
		String uri = eventSource.getDatasetUri(id);
		Util.assertDatasetExists(eventSource, NodeFactory.createURI(uri));

		String query = datasetInfoQuery.replaceAll("\\$dataset", "<" + uri + ">");
		Graph info = Util.queryDataStore(eventSource, query);
		String queryMerged = currentMergedRevisionsQuery.replaceAll("\\$dataset", "<" + uri + ">");
		GraphUtil.addInto(info, Util.queryDataStore(eventSource, queryMerged));
		return info;
	}

	@RequestMapping(value="/{id}/history", method=RequestMethod.GET)
	@ResponseBody
	public Graph history(@PathVariable String id) {
		d_log.debug("Dataset GET " + id + "/history");

		String uri = eventSource.getDatasetUri(id);
		Util.assertDatasetExists(eventSource, NodeFactory.createURI(uri));

		String query = datasetHistoryQuery.replaceAll("\\$dataset", "<" + uri + ">");
		Graph history = Util.queryDataStore(eventSource, query);
		String queryMerged = allMergedRevisionsQuery.replaceAll("\\$dataset", "<" + uri + ">");
		GraphUtil.addInto(history, Util.queryDataStore(eventSource, queryMerged));
		return history;
	}
}
