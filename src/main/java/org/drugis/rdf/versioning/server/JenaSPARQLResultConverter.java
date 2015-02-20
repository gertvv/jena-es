package org.drugis.rdf.versioning.server;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.jena.riot.WebContent;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;

import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.sparql.resultset.SPARQLResult;

public class JenaSPARQLResultConverter extends AbstractHttpMessageConverter<SPARQLResult> {

	@Override
	protected boolean supports(Class<?> clazz) {
		return clazz.equals(SPARQLResult.class);
	}
	
	@Override
	public List<MediaType> getSupportedMediaTypes() {
		return Collections.singletonList(MediaType.parseMediaType("application/json"));
	}

	@Override
	protected SPARQLResult readInternal(Class<? extends SPARQLResult> clazz,
			HttpInputMessage inputMessage) throws IOException,
			HttpMessageNotReadableException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected void writeInternal(SPARQLResult t, HttpOutputMessage outputMessage)
			throws IOException, HttpMessageNotWritableException {
		outputMessage.getHeaders().setContentType(MediaType.parseMediaType(WebContent.contentTypeResultsJSON));
		ResultSetFormatter.outputAsJSON(outputMessage.getBody(), t.getResultSet());
	}

}
