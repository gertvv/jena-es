package org.drugis.rdf.versioning.server;

import java.util.Observable;
import java.util.Observer;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.ReadWrite;

import es.DatasetGraphEventSourcing;

public class Util {

	/**
	 * Run an action in a WRITE transaction and return the newly created version.
	 * @param dataset Event sourcing dataset to run the action on.
	 * @param version The version the dataset is expected to be (may be null for no check). A VersionMismatchException is thrown if it doesn't match.
	 * @param action The write action to run.
	 * @return The newly created version.
	 */
	public static String runReturningVersion(DatasetGraphEventSourcing dataset, String version, Runnable action) {
		dataset.begin(ReadWrite.WRITE);
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

}
