package org.drugis.rdf.versioning.server;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.NOT_IMPLEMENTED)
public class NotImplementedException extends RuntimeException {
	private static final long serialVersionUID = -7785906341060652376L;

	public NotImplementedException(String message) {
		super(message);
	}
}
