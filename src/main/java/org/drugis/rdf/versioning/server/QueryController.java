package org.drugis.rdf.versioning.server;

import java.util.Arrays;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.drugis.rdf.versioning.server.messages.BooleanResult;
import org.drugis.rdf.versioning.server.messages.TransactionResultSet;
import org.drugis.rdf.versioning.store.DatasetGraphEventSourcing;
import org.drugis.rdf.versioning.store.DatasetNotFoundException;
import org.drugis.rdf.versioning.store.EventSource;
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
import com.hp.hpl.jena.query.QueryParseException;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.sparql.core.DatasetDescription;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.DynamicDatasets;
import com.hp.hpl.jena.sparql.core.Transactional;

@Controller
@RequestMapping("/datasets/{datasetId}/query")
public class QueryController {
	@Autowired EventSource d_eventSource;
	
	Log d_log = LogFactory.getLog(getClass());
	
	@RequestMapping(method={RequestMethod.GET, RequestMethod.HEAD})
	@ResponseBody
	public Object query(
			@PathVariable String datasetId,
			@RequestParam String query,
			@RequestParam(value="default-graph-uri", required=false) String[] defaultGraphUri,
			@RequestParam(value="named-graph-uri", required=false) String[] namedGraphUri,
			@RequestHeader(value="X-Accept-EventSource-Version", required=false) String version,
			HttpServletResponse response) {
		d_log.debug("Query " + datasetId);

		// Can not specify default of {} for @RequestParam.
		if (defaultGraphUri == null) {
			defaultGraphUri = new String[0];
		}
		if (namedGraphUri == null) {
			namedGraphUri = new String[0];
		}
		
		final DatasetGraphEventSourcing dataset = Util.getDataset(d_eventSource, datasetId);

		Query theQuery = null;
		try {
			theQuery = QueryFactory.create(query, Config.BASE_URI);
		} catch (QueryParseException e) {
			throw new RequestParseException(e);
		}
		DatasetGraph dsg = dataset;
		
		d_log.debug(query);

		QueryExecution qExec;
		try {
			dataset.begin(ReadWrite.READ);
			if (!dataset.isInTransaction()) {
				throw new DatasetNotFoundException(dataset.getDatasetUri());
			}
			d_log.debug("Opened READ transaction");
			
			// Get the correct version
			if (version != null) {
				dsg = dataset.getView(NodeFactory.createURI(version));
			} else {
				version = dataset.getLatestEvent().getURI();
			}
			if (dsg == null) {
				throw new VersionNotFoundException();
			}
			
			// Construct dataset supplied through request parameters
			if (defaultGraphUri.length > 0 || namedGraphUri.length > 0) {
				DatasetDescription description = new DatasetDescription(Arrays.asList(defaultGraphUri), Arrays.asList(namedGraphUri));
				dsg = DynamicDatasets.dynamicDataset(description, dsg, false);
			}
		
			response.setHeader("X-EventSource-Version", version);
			response.setHeader("Vary", "Accept, X-Accept-EventSource-Version");
			qExec = QueryExecutionFactory.create(theQuery, DatasetFactory.create(dsg));
		} catch (VersionNotFoundException e) {
			d_log.debug("Closing due to VersionNotFound");
			dataset.end();
			throw e;
		} catch (DatasetNotFoundException e) {
			// transaction already closed
			throw e;
		} catch (Exception e) {
			d_log.debug("Closing due to Exception prior to query: " + e);
			dataset.end();
			throw new RuntimeException(e);
		}

		return executeQuery(dataset, qExec, theQuery);
	}

	protected Object executeQuery(Transactional transactional, QueryExecution qExec, Query query) {
		if (query.isSelectType()) {
			ResultSet rs =  null;
			try {
				rs = qExec.execSelect();
				rs.hasNext();
			} catch (Exception e) {
				d_log.debug("Closing due to Exception in query: " + e);
				transactional.end();
				throw e;
			}
			return new TransactionResultSet(rs, transactional);
		}
		
		Object result = null;
		try {
			if (query.isConstructType()) {
				result = qExec.execConstruct().getGraph();
			} else if (query.isDescribeType()) {
				result = qExec.execDescribe().getGraph();
			} else if (query.isAskType()) {
				result = new BooleanResult(qExec.execAsk());
			}
		} finally {
			d_log.debug("Closing after query (finally block)");
			transactional.end();
		}
		return result;
	}
}