package org.drugis.rdf.versioning.server;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.NOT_ACCEPTABLE)
public class VersionNotFoundException extends RuntimeException {
	private static final long serialVersionUID = -203709238767089550L;
	
	public VersionNotFoundException() {
		super("The specified version could not be found");
	}

}
