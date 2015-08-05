package org.drugis.rdf.versioning.store;
import java.util.Observable;
import java.util.Observer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.shared.Lock;
import com.hp.hpl.jena.sparql.JenaTransactionException;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.DatasetGraphTrackActive;
import com.hp.hpl.jena.sparql.core.Transactional;
import com.hp.hpl.jena.sparql.graph.GraphFactory;
import com.hp.hpl.jena.sparql.util.Context;
import com.hp.hpl.jena.update.GraphStore;

/**
 * Event sourcing dataset that supports transactions (single writer, multiple reader).
 */
public class DatasetGraphEventSourcing extends DatasetGraphTrackActive implements GraphStore {
	Log d_log = LogFactory.getLog(getClass());
	
	private class Transaction extends Observable {
		public DatasetGraph dsg;
		public Graph meta;
		
		public Transaction(DatasetGraph dsg) {
			this.dsg = dsg;
			this.meta = GraphFactory.createGraphMem();
		}
		
		public void notifyVersion(Node version) {
			setChanged();
			notifyObservers(version);
		}
	}
	
	private EventSource d_eventSource;
	private Node d_dataset;
	private ThreadLocal<Transaction> d_txn;

	public DatasetGraphEventSourcing(EventSource eventSource, Node logUri) {
		d_eventSource = eventSource;
		if (!(eventSource.getDataStore() instanceof Transactional)) {
			throw new IllegalArgumentException("DatasetGraphEventSourcing can only be based on a Transactional DatasetGraph");
		}
		d_dataset = logUri;
		d_txn = new ThreadLocal<Transaction>();
	}
	
	public Node getLatestEvent() {
		return d_eventSource.getLatestVersionUri(d_dataset);
	}
	
	public boolean exists() {
		return d_eventSource.datasetExists(d_dataset);
	}
	
	@Override
	public Lock getLock() {
		return d_eventSource.getDataStore().getLock(); // assuming it is SWMR
	}

	@Override
	public Context getContext() {
		return Context.emptyContext;
	}

	@Override
	public Dataset toDataset() {
		return DatasetFactory.create(get());
	}

	@Override
	protected DatasetGraph get() {
		if (isInTransaction()) {
			return d_txn.get().dsg;
		}
		throw new IllegalAccessError("Not in a transaction");
	}

	@Override
	protected void checkActive() {
		if (!isInTransaction()) {
			throw new JenaTransactionException("Not in a transaction");
		}
	}

	@Override
	protected void checkNotActive() {
		if (isInTransaction()) {
			throw new JenaTransactionException("Already in a transaction");
		}
	}

	@Override
	public boolean isInTransaction() {
		return d_txn.get() != null;
	}
	
	public Graph getTransactionMetaGraph() {
		checkActive();
		return d_txn.get().meta;
	}
	
	public void addCommitListener(Observer o) {
		checkWrite();
		d_txn.get().addObserver(o);
	}

	@Override
	protected void _begin(ReadWrite readWrite) {
		d_log.debug("TRANSACTION BEGIN for " + readWrite);
		if (readWrite == ReadWrite.READ) { // read-only: construct a view
			getTransactional().begin(ReadWrite.READ);
			d_txn.set(new Transaction(getLatestVersionOrEndTransaction()));
		} else { // read-write
			getTransactional().begin(ReadWrite.WRITE);
			d_txn.set(new Transaction(new DatasetGraphDelta(getLatestVersionOrEndTransaction())));
		}
		
	}

	private DatasetGraph getLatestVersionOrEndTransaction() {
		try {
			return d_eventSource.getLatestVersion(d_dataset);
		} catch (RuntimeException e) {
			getTransactional().end();
			throw e;
		}
	}

	private Transactional getTransactional() {
		return (Transactional)d_eventSource.getDataStore();
	}
	
	private void checkWrite() {
		checkActive();
		if (!(d_txn.get().dsg instanceof DatasetGraphDelta)) {
			throw new JenaTransactionException("Operation not applicable to read-only transaction");
		}
	}

	@Override
	protected void _commit() {
		d_log.debug("TRANSACTION COMMIT");
		checkWrite();
		Node version = d_eventSource.writeToLog(d_dataset, (DatasetGraphDelta) d_txn.get().dsg, d_txn.get().meta);
		getTransactional().commit();
		d_txn.get().notifyVersion(version);
		d_txn.remove();
	}

	@Override
	protected void _abort() {
		d_log.debug("TRANSACTION ABORT");
		checkWrite();
		getTransactional().abort();
		d_txn.remove();
	}

	@Override
	protected void _end() {
		d_log.debug("TRANSACTION END");
		if (isInTransaction()) {
			getTransactional().end();
			d_txn.remove();
		} else {
			throw new RuntimeException("Trying to close non-existent transaction");
		}
	}

	@Override
	protected void _close() {
		d_eventSource.getDataStore().close();
	}

	@Override
	public void startRequest() {
		// NI
	}

	@Override
	public void finishRequest() {
		// NI
	}

	public DatasetGraph getView(Node version) {
		return d_eventSource.getVersion(d_dataset, version);
	}

	public Node getDatasetUri() {
		return d_dataset;
	}
}
