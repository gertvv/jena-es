package org.drugis.rdf.versioning.server;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
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
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.resultset.SPARQLResult;

import es.DatasetGraphEventSourcing;

@Controller
@RequestMapping("/datasets/{datasetId}/query")
public class QueryController {
	@Autowired DatasetGraphEventSourcing dataset;

	@RequestMapping(method=RequestMethod.GET)
	@ResponseBody
	public SPARQLResult query(
			@RequestParam String query,
			@RequestHeader(value="X-Accept-EventSource-Version", required=false) String version) {
		System.out.println(query);
		Query theQuery = QueryFactory.create(query);
		DatasetGraph dsg = dataset;
		dataset.begin(ReadWrite.READ);
		if (version != null) {
			dsg = dataset.getView(NodeFactory.createURI(version));
		}
		try {
			QueryExecution qExec = QueryExecutionFactory.create(theQuery, DatasetFactory.create(dsg));
			return executeQuery(qExec, theQuery);
		} finally {
			dataset.end();
		}
	}

	protected SPARQLResult executeQuery(QueryExecution qExec, Query query) {
		if (query.isSelectType()) {
			ResultSet rs = qExec.execSelect();
			rs.hasNext();
			return new SPARQLResult(rs);
		}

		if (query.isConstructType()) {
			Model model = qExec.execConstruct() ;
			return new SPARQLResult(model) ;
		}

		if (query.isDescribeType()) {
			Model model = qExec.execDescribe() ;
			return new SPARQLResult(model) ;
		}

		if ( query.isAskType() ) {
			boolean b = qExec.execAsk() ;
			return new SPARQLResult(b) ;
		}

		return null;
	}
}