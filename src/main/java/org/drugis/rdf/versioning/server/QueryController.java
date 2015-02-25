package org.drugis.rdf.versioning.server;

import javax.servlet.http.HttpServletResponse;

import org.drugis.rdf.versioning.server.messages.BooleanResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.sparql.core.DatasetGraph;

import es.DatasetGraphEventSourcing;

@Controller
@RequestMapping("/datasets/{datasetId}/query")
public class QueryController {
	@Autowired DatasetGraph d_eventSource;

	@RequestMapping(method={RequestMethod.GET, RequestMethod.HEAD})
	@ResponseBody
	public Object query(
			@PathVariable String datasetId,
			@RequestParam String query,
			@RequestHeader(value="X-Accept-EventSource-Version", required=false) String version,
			HttpServletResponse response) {
		final DatasetGraphEventSourcing dataset = Util.getDataset(d_eventSource, datasetId);

		System.out.println(query);
		Query theQuery = QueryFactory.create(query);
		DatasetGraph dsg = dataset;
		try {
			dataset.begin(ReadWrite.READ);
			if (version != null) {
				dsg = dataset.getView(NodeFactory.createURI(version));
			} else {
				version = dataset.getLatestEvent().getURI();
			}
			response.setHeader("X-EventSource-Version", version);
			response.setHeader("Vary", "Accept, X-Accept-EventSource-Version");
			QueryExecution qExec = QueryExecutionFactory.create(theQuery, DatasetFactory.create(dsg));
			return executeQuery(qExec, theQuery);
		} finally {
			dataset.end();
		}
	}

	protected Object executeQuery(QueryExecution qExec, Query query) {
		if (query.isSelectType()) {
			ResultSet rs = qExec.execSelect();
			rs.hasNext();
			return rs;
		}

		if (query.isConstructType()) {
			return qExec.execConstruct().getGraph();
		}

		if (query.isDescribeType()) {
			return qExec.execDescribe().getGraph();
		}

		if (query.isAskType()) {
			return new BooleanResult(qExec.execAsk());
		}

		return null;
	}
}