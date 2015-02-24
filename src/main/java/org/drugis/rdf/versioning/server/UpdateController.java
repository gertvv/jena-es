package org.drugis.rdf.versioning.server;

import java.util.Observable;
import java.util.Observer;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.hp.hpl.jena.graph.Node;
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
		if (version != null && !version.equals(dataset.getLatestEvent().getURI())) {
			System.err.println(version);
			System.err.println(dataset.getLatestEvent().getURI());
			dataset.abort();
			throw new VersionMismatchException();
		}
		try {
			UsingList usingList = new UsingList();
			final Node[] newVersion = { null };
			dataset.addCommitListener(new Observer() {
				@Override
				public void update(Observable o, Object arg) {
					newVersion[0] = (Node) arg;
				}});
			UpdateAction.parseExecute(usingList, dataset, request.getInputStream(), UpdateParseBase, Syntax.syntaxARQ);
			dataset.commit();
			response.setHeader("X-EventSource-Version", newVersion[0].getURI());
			return null;
		} catch (Exception e) {
			dataset.abort();
			throw e;
		}
	}
}
