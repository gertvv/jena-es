import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.shared.Lock;
import com.hp.hpl.jena.sparql.JenaTransactionException;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.DatasetGraphTrackActive;
import com.hp.hpl.jena.sparql.util.Context;
import com.hp.hpl.jena.update.GraphStore;

/**
 * Event sourcing dataset that supports transactions (single writer, multiple reader).
 */
public class DatasetGraphEventSourcing extends DatasetGraphTrackActive implements GraphStore {
	
	private DatasetGraph d_eventSource;
	private Node d_logUri;
	private ThreadLocal<DatasetGraph> d_txn;

	public DatasetGraphEventSourcing(DatasetGraph eventSource, Node logUri) {
		d_eventSource = eventSource;
		d_logUri = logUri;
		d_txn = new ThreadLocal<DatasetGraph>();
	}
	
	@Override
	public Lock getLock() {
		return d_eventSource.getLock(); // assuming it is SWMR
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
			return d_txn.get();
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

	@Override
	protected void _begin(ReadWrite readWrite) {
		if (readWrite == ReadWrite.READ) { // read-only: construct a view
			getLock().enterCriticalSection(Lock.READ);
			d_txn.set(EventSource2.replayLog(d_eventSource, d_logUri));
		} else { // read-write
			getLock().enterCriticalSection(Lock.WRITE);
			d_txn.set(new DatasetGraphDelta(EventSource2.replayLog(d_eventSource, d_logUri)));
		}
		
	}
	
	private void checkWrite() {
		checkActive();
		if (!(d_txn.get() instanceof DatasetGraphDelta)) {
			throw new JenaTransactionException("Operation not applicable to read-only transaction");
		}
	}

	@Override
	protected void _commit() {
		checkWrite();
		EventSource2.writeToLog(d_eventSource, d_logUri, (DatasetGraphDelta) d_txn.get());
		d_txn.remove();
		getLock().leaveCriticalSection();
	}

	@Override
	protected void _abort() {
		checkWrite();
		d_txn.remove();
		getLock().leaveCriticalSection();
	}

	@Override
	protected void _end() {
		if (isInTransaction()) {
			d_txn.remove();
			getLock().leaveCriticalSection();
		}
	}

	@Override
	protected void _close() {
		d_eventSource.close();
	}

	@Override
	public void startRequest() {
		// NI
	}

	@Override
	public void finishRequest() {
		// NI
	}
}
