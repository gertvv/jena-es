package org.drugis.rdf.versioning.server;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.BAD_REQUEST)
public class RequestParseException extends RuntimeException {
	private static final long serialVersionUID = -3322967593518077203L;

	public RequestParseException(Throwable cause) {
		super(cause);
	}
}
