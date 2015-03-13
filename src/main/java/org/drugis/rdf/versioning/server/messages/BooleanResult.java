package org.drugis.rdf.versioning.server.messages;

public class BooleanResult {
	private final boolean d_value;

	public BooleanResult(boolean value) {
		d_value = value;
	}

	public boolean getValue() {
		return d_value;
	}
}
