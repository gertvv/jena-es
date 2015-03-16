package org.drugis.rdf.versioning.server;

import org.drugis.rdf.versioning.store.EventSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.hp.hpl.jena.graph.Graph;

@Controller
@RequestMapping("/versions")
public class VersionController {
	@Autowired EventSource eventSource;
	@Autowired String versionInfoQuery;

	@RequestMapping(value="/{id}", method=RequestMethod.GET)
	@ResponseBody
	public Graph get(@PathVariable String id) {
		String query = versionInfoQuery.replaceAll("\\$version", "<" + eventSource.getUriPrefix() + "versions/" + id + ">");
		return Util.queryDataStore(eventSource, query);
	}
}
