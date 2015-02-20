package org.drugis.rdf.versioning.server;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/datasets")
public class DatasetController {
	@RequestMapping(value="", method=RequestMethod.GET, produces="text/html")
	@ResponseBody
	public String list() {
		return "<a href=\"/datasets/hello\">Only dataset</a>";
	}

	@RequestMapping(value="/{id}", method=RequestMethod.GET, produces="text/html")
	@ResponseBody
	public String get(@PathVariable String id) {
		return id;
	}
}
