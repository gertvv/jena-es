package org.drugis.rdf.versioning.server;

import java.util.Iterator;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.drugis.rdf.versioning.server.GraphStoreController.TargetGraph;
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
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.sparql.core.DatasetGraph;

@Controller
@RequestMapping("/datasets/{datasetId}/dump")
public class DumpController {
	@Autowired EventSource d_eventSource;
	Log d_log = LogFactory.getLog(getClass());
	
	@RequestMapping(method={RequestMethod.GET, RequestMethod.HEAD})
	@ResponseBody
	public DatasetGraph get(
			@PathVariable String datasetId,
			@RequestHeader(value=ESHeaders.ACCEPT_VERSION, required=false) String version,
			HttpServletResponse response) {
		final DatasetGraphEventSourcing dataset = Util.getDataset(d_eventSource, datasetId);

		d_log.debug("Dump GET " + datasetId);

		dataset.begin(ReadWrite.READ);
		try {
			DatasetGraph rval;
			if (version == null) {
				rval = dataset;
				version = dataset.getLatestEvent().getURI();
			} else {
				rval = dataset.getView(NodeFactory.createURI(version));
				if (rval == null) {
					throw new VersionNotFoundException();
				}
			}
			response.setHeader(ESHeaders.VERSION, version);
			response.setHeader(HttpHeaders.VARY, HttpHeaders.ACCEPT + ", " + ESHeaders.ACCEPT_VERSION);
			return rval;
		} finally {
//			dataset.end(); -- make the MessageConverter close it.
		}
	}
	
	@RequestMapping(method=RequestMethod.PUT)
	public void put(
			@PathVariable String datasetId,
			@RequestHeader(value="X-Accept-EventSource-Version", required=false) String version,
			final @RequestBody DatasetGraph data,
			HttpServletRequest request,
			HttpServletResponse response) {
		final DatasetGraphEventSourcing dataset = Util.getDataset(d_eventSource, datasetId);

		d_log.debug("Dump PUT " + datasetId);

		Runnable action = new Runnable() {
			@Override
			public void run() {
				dataset.clear();
				GraphUtil.addInto(dataset.getDefaultGraph(), data.getDefaultGraph());
				for (Iterator<Node> i = data.listGraphNodes(); i.hasNext(); ) {
					Node g = i.next();
					dataset.addGraph(g, data.getGraph(g));
				}
			}
		};
		String newVersion = Util.runReturningVersion(dataset, version, action, Util.versionMetaData(request));
		response.setHeader("X-EventSource-Version", newVersion);
	}
}
