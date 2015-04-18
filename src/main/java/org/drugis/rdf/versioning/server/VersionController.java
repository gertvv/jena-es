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

@Controller
@RequestMapping("/versions")
public class VersionController {
	@Autowired EventSource eventSource;
	@Autowired String versionInfoQuery;
	Log d_log = LogFactory.getLog(getClass());

	@RequestMapping(value="/{id}", method=RequestMethod.GET)
	@ResponseBody
	public Graph get(@PathVariable String id) {
		d_log.debug("Version GET " + id);
		String query = versionInfoQuery.replaceAll("\\$version", "<" + eventSource.getVersionUri(id) + ">");
		return Util.queryDataStore(eventSource, query);
	}
}
