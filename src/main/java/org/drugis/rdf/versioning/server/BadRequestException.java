package org.drugis.rdf.versioning.server;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.BAD_REQUEST)
public class BadRequestException extends RuntimeException {
	private static final long serialVersionUID = -3333929426939009961L;

	public BadRequestException(String message) {
		super(message);
	}
}
