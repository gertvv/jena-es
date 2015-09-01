package org.drugis.rdf.versioning.server;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Observable;
import java.util.Observer;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.codec.binary.Base64;
import org.drugis.rdf.versioning.store.DatasetGraphEventSourcing;
import org.drugis.rdf.versioning.store.DatasetNotFoundException;
import org.drugis.rdf.versioning.store.EventSource;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.GraphUtil;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.query.Syntax;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.sparql.core.Transactional;
import com.hp.hpl.jena.sparql.graph.GraphFactory;
import com.hp.hpl.jena.vocabulary.RDF;

public class Util {
	/**
	 * Run an action in a WRITE transaction and return the newly created version.
	 * @param dataset Event sourcing dataset to run the action on.
	 * @param version The version the dataset is expected to be (may be null for no check). A VersionMismatchException is thrown if it doesn't match.
	 * @param action The write action to run.
	 * @return The newly created version.
	 */
	public static String runReturningVersion(DatasetGraphEventSourcing dataset, String version, Runnable action, Graph meta) {
		dataset.begin(ReadWrite.WRITE);
		GraphUtil.addInto(dataset.getTransactionMetaGraph(), meta);
		if (version != null && !version.equals(dataset.getLatestEvent().getURI())) {
			dataset.abort();
			throw new VersionMismatchException();
		}
		try {
			final String[] newVersion = { null };
			dataset.addCommitListener(new Observer() {
				@Override
				public void update(Observable o, Object arg) {
					newVersion[0] = ((Node) arg).getURI();
				}});
			action.run();
			dataset.commit();
			return newVersion[0];
		} catch (Exception e) {
			dataset.abort();
			throw e;
		}
	}

	public static DatasetGraphEventSourcing getDataset(EventSource eventSource, String datasetId) {
		return new DatasetGraphEventSourcing(eventSource, NodeFactory.createURI(eventSource.getDatasetUri(datasetId)));
	}

	public static Graph queryDataStore(EventSource eventSource, String query) {
		Query theQuery = QueryFactory.create(query, Syntax.syntaxARQ);
	
		Transactional transactional = (Transactional)eventSource.getDataStore();
		transactional.begin(ReadWrite.READ);
		try {
			QueryExecution qExec = QueryExecutionFactory.create(theQuery, DatasetFactory.create(eventSource.getDataStore()));
			Model model = qExec.execConstruct();
			return model.getGraph();
		} finally {
			transactional.end();
		}
	}

	public static Graph getDataStoreGraph(EventSource eventSource, Node uri) {
		Transactional transactional = (Transactional)eventSource.getDataStore();
		transactional.begin(ReadWrite.READ);
		Graph graph = eventSource.getDataStore().getGraph(uri);
		transactional.end();
		return graph;
	}
	
	public static void assertDatasetExists(EventSource eventSource, Node dataset) {
		Transactional transactional = (Transactional)eventSource.getDataStore();
		transactional.begin(ReadWrite.READ);
		boolean exists = eventSource.datasetExists(dataset);
		transactional.end();
		if (!exists) {
			throw new DatasetNotFoundException(dataset);
		}
	}

	static String decodeHeader(String value) {
		return new String(Base64.decodeBase64(value), StandardCharsets.UTF_8);
	}

	static Graph versionMetaData(HttpServletRequest request) {
		Graph graph = GraphFactory.createGraphMem();
		
		Node root = NodeFactory.createAnon();
		graph.add(new Triple(root, RDF.Nodes.type, EventSource.esClassDatasetVersion));
		
		String creator = request.getHeader("X-EventSource-Creator");
		if (creator != null) {
			graph.add(new Triple(root, EventSource.dctermsCreator, NodeFactory.createURI(creator)));
		}
		
		String title = request.getHeader("X-EventSource-Title");
		if (title != null) {
			title = decodeHeader(title);
			graph.add(new Triple(root, EventSource.dctermsTitle, NodeFactory.createLiteral(title)));
		}
		
		String description = request.getHeader("X-EventSource-Description");
		if (description != null) {
			description = decodeHeader(description);
			graph.add(new Triple(root, EventSource.dctermsDescription, NodeFactory.createLiteral(description)));
		}
		
		return graph;
	}

	public static Node getUniqueOptionalObject(Iterator<Triple> result) {
		if (result.hasNext()) {
			Node object = result.next().getObject();
			if (result.hasNext()) {
				throw new IllegalStateException("Multiple subjects on property of arity 1");
			}
			return object;
		}
		return null;
	}

	public static Node getUniqueObject(Iterator<Triple> result) {
		Node object = getUniqueOptionalObject(result);
		if (object == null) {
			throw new IllegalStateException("Zero subjects on property of arity 1");
		}
		return object;
	}

	public static Node getUniqueOptionalSubject(Iterator<Triple> result) {
		if (result.hasNext()) {
			Node subject = result.next().getSubject();
			if (result.hasNext()) {
				throw new IllegalStateException("Multiple subjects on property of arity 1");
			}
			return subject;
		}
		return null;
	}

}
