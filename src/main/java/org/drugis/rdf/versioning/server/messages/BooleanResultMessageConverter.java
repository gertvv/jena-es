package org.drugis.rdf.versioning.server.messages;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;

import com.hp.hpl.jena.query.ResultSetFormatter;

public class BooleanResultMessageConverter extends AbstractHttpMessageConverter<BooleanResult> {
	private static List<MediaType> s_supported = new ArrayList<MediaType>();
	private static Map<MediaType, Lang> s_lang = new HashMap<MediaType, Lang>();
	static {
		Lang [] langs = {
				ResultSetLang.SPARQLResultSetJSON,
				ResultSetLang.SPARQLResultSetCSV,
				ResultSetLang.SPARQLResultSetTSV,
				ResultSetLang.SPARQLResultSetXML,
				ResultSetLang.SPARQLResultSetText
		};
		ResultSetUtil.setSupportedMediaTypes(langs, s_supported, s_lang);
	}

	@Override
	protected boolean supports(Class<?> clazz) {
		return BooleanResult.class.isAssignableFrom(clazz);
	}
	
	@Override
	public List<MediaType> getSupportedMediaTypes() {
		return s_supported;
	}
	
	@Override
	protected boolean canRead(MediaType mediaType) {
		return false;
	}

	@Override
	protected BooleanResult readInternal(Class<? extends BooleanResult> clazz, HttpInputMessage inputMessage)
			throws IOException, HttpMessageNotReadableException {
		throw new UnsupportedOperationException("Can only write SPARQL results");
	}

	@Override
	protected void writeInternal(BooleanResult result, HttpOutputMessage outputMessage)
			throws IOException, HttpMessageNotWritableException {

		Lang lang = s_lang.get(outputMessage.getHeaders().getContentType());
		OutputStream out = outputMessage.getBody();
		boolean val = result.getValue();

		if (lang.equals(ResultSetLang.SPARQLResultSetJSON)) {
			ResultSetFormatter.outputAsJSON(out, val);
		} else if (lang.equals(ResultSetLang.SPARQLResultSetCSV)) {
			ResultSetFormatter.outputAsCSV(out, val);
		} else if (lang.equals(ResultSetLang.SPARQLResultSetTSV)) {
			ResultSetFormatter.outputAsTSV(out, val);
		} else if (lang.equals(ResultSetLang.SPARQLResultSetXML)) {
			ResultSetFormatter.outputAsXML(out, val);
		} else if (lang.equals(ResultSetLang.SPARQLResultSetText)) {
			ResultSetFormatter.out(out, val);
		}
	}
}
