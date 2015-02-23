package org.drugis.rdf.versioning.server;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.CONFLICT)
public class VersionMismatchException extends RuntimeException {
	private static final long serialVersionUID = 3344439242669274562L;

	public VersionMismatchException() {
		super("An update request failed because the specified version didn't match the current version");
	}
}
