package org.drugis.rdf.versioning.server;

import java.util.UUID;

import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;

import es.EventSource;

@Controller
@RequestMapping("/datasets")
public class DatasetController {
	@Autowired EventSource eventSource;

	@Autowired
	@RequestMapping(value="", method=RequestMethod.GET, produces="text/html")
	@ResponseBody
	public String list() {
		return "<a href=\"/datasets/hello\">Only dataset</a>";
	}
	
	@RequestMapping(value="", method=RequestMethod.POST)
	@ResponseStatus(HttpStatus.CREATED)
	public void create(HttpServletResponse response) {
		String id = UUID.randomUUID().toString();
		Node dataset = NodeFactory.createURI("http://example.com/datasets/" + id);
		eventSource.createDatasetIfNotExists(dataset);
		response.setHeader(HttpHeaders.LOCATION, dataset.getURI());
	}

	@RequestMapping(value="/{id}", method=RequestMethod.GET, produces="text/html")
	@ResponseBody
	public String get(@PathVariable String id) {
		return id;
	}
}
