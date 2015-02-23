package org.drugis.rdf.versioning.server;

import java.util.Collections;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.query.ReadWrite;

import es.DatasetGraphEventSourcing;

@Controller
@RequestMapping("/datasets/{datasetId}/data")
public class GraphStoreController {
	@Autowired DatasetGraphEventSourcing dataset;

	@RequestMapping(method=RequestMethod.GET)
	@ResponseBody
	public Graph get(
			@RequestParam Map<String,String> params,
			@RequestHeader(value="X-Accept-EventSource-Version", required=false) String version,
			HttpServletResponse response) {
		if (params.keySet().equals(Collections.singleton("default")) && params.get("default").equals("")) {
			return null;
		} else if (params.keySet().equals(Collections.singleton("graph"))) {
			Node graphNode = NodeFactory.createURI(params.get("graph"));
			dataset.begin(ReadWrite.READ);
			Graph rval;
			if (version == null) {
				rval = dataset.getGraph(graphNode);
				version = dataset.getLatestEvent().getURI();
			} else {
				rval = dataset.getView(NodeFactory.createURI(version)).getGraph(graphNode);
			}
			response.setHeader("X-EventSource-Version", version);
			response.setHeader("Vary", "Accept, X-Accept-EventSource-Version");
			dataset.end();
			return rval;
		} else {
			throw new InvalidGraphSpecificationException();
		}
	}
}
