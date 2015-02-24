package org.drugis.rdf.versioning.server.messages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RDFWriterRegistry;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.sparql.graph.GraphFactory;

public class JenaGraphMessageConverter extends AbstractHttpMessageConverter<Graph> {
	private static MediaType s_turtle = MediaType.parseMediaType("text/turtle");
	private static List<MediaType> s_supported = new ArrayList<MediaType>();
	static {
		// Add all supported languages
		for (Lang lang : RDFLanguages.getRegisteredLanguages()) {
			if (RDFLanguages.isTriples(lang)) {
				s_supported.add(MediaType.parseMediaType(lang.getContentType().getContentType()));
			}
		}
		// Make turtle the default (if it exists)
		int idx = s_supported.indexOf(s_turtle);
		if (idx > 0) {
			s_supported.set(idx, s_supported.get(0));
			s_supported.set(0, s_turtle);
		}
	}

	private String d_base = "http://example.com/"; // FIXME

	@Override
	protected boolean supports(Class<?> clazz) {
		return Graph.class.isAssignableFrom(clazz);
	}

	@Override
	public List<MediaType> getSupportedMediaTypes() {
		return s_supported;
	}
	
	@Override
	protected MediaType getDefaultContentType(Graph t) throws IOException {
		return s_turtle;
	}

	@Override
	protected Graph readInternal(Class<? extends Graph> clazz, HttpInputMessage inputMessage)
	throws IOException, HttpMessageNotReadableException {
		Graph graph = GraphFactory.createGraphMem();
		RDFDataMgr.read(graph, inputMessage.getBody(), d_base, determineRDFLang(inputMessage.getHeaders()));
		return graph;
	}

	@Override
	protected void writeInternal(Graph graph, HttpOutputMessage outputMessage)
	throws IOException, HttpMessageNotWritableException {
		RDFFormat fmt = determineRDFFormat(outputMessage.getHeaders());
		RDFDataMgr.write(outputMessage.getBody(), graph, fmt);
	}

	private RDFFormat determineRDFFormat(HttpHeaders headers) {
		Lang lang = determineRDFLang(headers);
		return (lang == Lang.RDFXML) ? RDFFormat.RDFXML_PLAIN : RDFWriterRegistry.defaultSerialization(lang);
	}

	private Lang determineRDFLang(HttpHeaders headers) {
		MediaType mediaType = headers.getContentType();
		mediaType = new MediaType(mediaType.getType(), mediaType.getSubtype()); // strip all information not understood by RDFLanguages
		return RDFLanguages.contentTypeToLang(mediaType.toString());
	}
}