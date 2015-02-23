package org.drugis.rdf.versioning.server;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.hp.hpl.jena.query.QueryParseException;

@ControllerAdvice
public class ExceptionHandling {
	@ExceptionHandler(QueryParseException.class) 
	@ResponseStatus(HttpStatus.BAD_REQUEST)
	@ResponseBody
	public String queryParseException(QueryParseException e) {
		return e.getMessage() + "\n";
	}
}
