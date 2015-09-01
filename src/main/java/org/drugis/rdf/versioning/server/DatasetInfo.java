package org.drugis.rdf.versioning.server;

public class DatasetInfo {
	public final String id, head, creator;
	
	public DatasetInfo(String id, String head, String creator) {
		this.id = id;
		this.head = head;
		this.creator = creator;
	}
}
