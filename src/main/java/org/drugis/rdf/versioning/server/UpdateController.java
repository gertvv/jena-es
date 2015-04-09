package org.drugis.rdf.versioning.server;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jena.riot.WebContent;
import org.drugis.rdf.versioning.store.DatasetGraphEventSourcing;
import org.drugis.rdf.versioning.store.EventSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.query.QueryParseException;
import com.hp.hpl.jena.query.Syntax;
import com.hp.hpl.jena.sparql.modify.UsingList;
import com.hp.hpl.jena.update.UpdateAction;

@Controller
@RequestMapping("/datasets/{datasetId}/update")
public class UpdateController {
	@Autowired EventSource d_eventSource;
	Log d_log = LogFactory.getLog(getClass());

	@RequestMapping(method=RequestMethod.POST, consumes=WebContent.contentTypeSPARQLUpdate)
	public Object update(
			@PathVariable String datasetId,
			final HttpServletRequest request,
			@RequestParam(value="using-graph-uri", required=false) String[] usingGraphUri,
			@RequestParam(value="using-named-graph-uri", required=false) String[] usingNamedGraphUri,
			@RequestHeader(value="X-Accept-EventSource-Version", required=false) String version,
			HttpServletResponse response)
			throws Exception { // TODO: request parameters
		d_log.debug("Update " + datasetId);
	
		final DatasetGraphEventSourcing dataset = Util.getDataset(d_eventSource, datasetId);
		final UsingList usingList = new UsingList();
		
		// Can not specify default of {} for @RequestParam.
		if (usingGraphUri == null) {
			usingGraphUri = new String[0];
		}
		if (usingNamedGraphUri == null) {
			usingNamedGraphUri = new String[0];
		}
		for (String uri : usingGraphUri) {
			usingList.addUsing(NodeFactory.createURI(uri));
		}
		for (String uri : usingNamedGraphUri) {
			usingList.addUsingNamed(NodeFactory.createURI(uri));
		}

		Runnable action = new Runnable() {
			@Override
			public void run() {
				try {
					UpdateAction.parseExecute(usingList, dataset, request.getInputStream(), Config.BASE_URI, Syntax.syntaxARQ);
				} catch (QueryParseException e) {
					throw new RequestParseException(e);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		};

		String newVersion = Util.runReturningVersion(dataset, version, action, Util.versionMetaData(request));
		response.setHeader("X-EventSource-Version", newVersion);
		return null;
	}
}
