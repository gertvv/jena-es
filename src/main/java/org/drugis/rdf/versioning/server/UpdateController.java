package org.drugis.rdf.versioning.server;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.query.Syntax;
import com.hp.hpl.jena.sparql.modify.UsingList;
import com.hp.hpl.jena.update.UpdateAction;

import es.DatasetGraphEventSourcing;

@Controller
@RequestMapping("/datasets/{datasetId}/update")
public class UpdateController {
	@Autowired DatasetGraphEventSourcing dataset;

	private static final String UpdateParseBase = "http://example/update-base/"; // FIXME

	@RequestMapping(method=RequestMethod.POST, consumes="application/sparql-update")
	public Object update(
			HttpServletRequest request,
			@RequestHeader(value="X-Accept-EventSource-Version", required=false) String version,
			HttpServletResponse response)
			throws Exception { // TODO: request parameters
		dataset.begin(ReadWrite.WRITE);
		if (version != null && version != dataset.getLatestEvent().getURI()) {
			dataset.abort();
			throw new VersionMismatchException();
		}
		try {
			UsingList usingList = new UsingList();
			UpdateAction.parseExecute(usingList, dataset, request.getInputStream(), UpdateParseBase, Syntax.syntaxARQ);
			// TODO: find out the new version & return it
		} catch (Exception e) {
			dataset.abort();
			throw e;
		} finally {
			dataset.end();
		}
		return null;
	}
}
