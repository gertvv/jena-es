package org.drugis.rdf.versioning.server;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.drugis.rdf.versioning.store.EventSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.NodeFactory;

@Controller
public class DiffController {
	@Autowired EventSource eventSource;
	Log d_log = LogFactory.getLog(getClass());

	@RequestMapping(value="/assert/{id}", method=RequestMethod.GET)
	@ResponseBody
	public Graph getAssert(@PathVariable String id) {
		d_log.debug("Diff GET assert/" + id);
		return Util.getDataStoreGraph(eventSource, NodeFactory.createURI(eventSource.getUriPrefix() + "assert/" + id));
	}

	@RequestMapping(value="/retract/{id}", method=RequestMethod.GET)
	@ResponseBody
	public Graph getRetract(@PathVariable String id) {
		d_log.debug("Diff GET retract/" + id);
		return Util.getDataStoreGraph(eventSource, NodeFactory.createURI(Config.BASE_URI + "/retract/" + id));
	}
}
