package org.drugis.rdf.versioning.store;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.hp.hpl.jena.graph.Node;

@ResponseStatus(HttpStatus.NOT_FOUND)
public class DatasetNotFoundException extends RuntimeException {
	private static final long serialVersionUID = 1968960903282125914L;

	public DatasetNotFoundException(Node dataset) {
		super("Dataset <" + dataset.getURI() + "> not found");
	}

}
