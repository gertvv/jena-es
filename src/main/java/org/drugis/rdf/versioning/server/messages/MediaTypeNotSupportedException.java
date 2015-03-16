package org.drugis.rdf.versioning.server.messages;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.NOT_ACCEPTABLE)
public class MediaTypeNotSupportedException extends RuntimeException {
	private static final long serialVersionUID = 4754381993336060897L;

}
