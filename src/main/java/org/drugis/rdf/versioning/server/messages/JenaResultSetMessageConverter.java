package org.drugis.rdf.versioning.server.messages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.ResultSetMgr;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;

import com.hp.hpl.jena.query.ResultSet;

public class JenaResultSetMessageConverter extends AbstractHttpMessageConverter<ResultSet> {
	Log d_log = LogFactory.getLog(getClass());
	
	private static List<MediaType> s_supported = new ArrayList<MediaType>();
	private static Map<MediaType, Lang> s_lang = new HashMap<MediaType, Lang>();
	static {
		Lang [] langs = {
				ResultSetLang.SPARQLResultSetJSON,
				ResultSetLang.SPARQLResultSetCSV,
				ResultSetLang.SPARQLResultSetTSV,
				ResultSetLang.SPARQLResultSetXML,
				ResultSetLang.SPARQLResultSetText,
				ResultSetLang.SPARQLResultSetThrift
		};
		ResultSetUtil.setSupportedMediaTypes(langs, s_supported, s_lang);
		s_supported.add(MediaType.ALL); // Make sure we get all ResultSets so we can close them
	}

	@Override
	protected boolean supports(Class<?> clazz) {
		return ResultSet.class.isAssignableFrom(clazz);
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
	protected ResultSet readInternal(Class<? extends ResultSet> clazz, HttpInputMessage inputMessage)
			throws IOException, HttpMessageNotReadableException {
		throw new UnsupportedOperationException("Can only write SPARQL results");
	}

	@Override
	protected void writeInternal(ResultSet rs, HttpOutputMessage outputMessage)
			throws IOException, HttpMessageNotWritableException {
		Lang lang = s_lang.get(outputMessage.getHeaders().getContentType());
		if (lang == null) {
			if (rs instanceof TransactionResultSet) {
				d_log.debug("Closing transaction due to unsupported media type");
				((TransactionResultSet) rs).endTransaction();
			}
			throw new MediaTypeNotSupportedException();
		}
		try {
			ResultSetMgr.write(outputMessage.getBody(), rs, lang);
		} finally {
			if (rs instanceof TransactionResultSet) {
				d_log.debug("Closing transaction in MessageConverter");
				((TransactionResultSet) rs).endTransaction();
			}
		}
	}
}
