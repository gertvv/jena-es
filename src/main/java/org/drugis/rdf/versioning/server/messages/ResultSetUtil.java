package org.drugis.rdf.versioning.server.messages;

import java.util.List;
import java.util.Map;

import org.apache.jena.riot.Lang;
import org.springframework.http.MediaType;

public class ResultSetUtil {

	public static void setSupportedMediaTypes(Lang[] langs, List<MediaType> types, Map<MediaType, Lang> map) {
		for (Lang lang : langs) {
			MediaType mediaType = MediaType.parseMediaType(lang.getContentType().getContentType());
			types.add(mediaType);
			map.put(mediaType, lang);
			for (String str : lang.getAltContentTypes()) {
				map.put(MediaType.parseMediaType(str), lang);
			}
		}
	}

}
