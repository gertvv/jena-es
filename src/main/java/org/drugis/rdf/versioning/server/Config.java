package org.drugis.rdf.versioning.server;

import java.util.List;

import org.drugis.rdf.versioning.server.messages.BooleanResultMessageConverter;
import org.drugis.rdf.versioning.server.messages.JenaGraphMessageConverter;
import org.drugis.rdf.versioning.server.messages.JenaResultSetMessageConverter;
import org.springframework.boot.autoconfigure.web.WebMvcAutoConfiguration.WebMvcAutoConfigurationAdapter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.web.filter.CharacterEncodingFilter;

import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.tdb.TDBFactory;

@Configuration
public class Config extends WebMvcAutoConfigurationAdapter {
	@Bean
	public DatasetGraph eventSource() {
		return TDBFactory.createDatasetGraph("DB");
	}

	@Bean
	CharacterEncodingFilter characterEncodingFilter() {
		CharacterEncodingFilter filter = new CharacterEncodingFilter();
		filter.setEncoding("UTF-8");
		filter.setForceEncoding(true);
		return filter;
	} 
	
    @Override
    public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
        converters.add(new JenaGraphMessageConverter());
        converters.add(new JenaResultSetMessageConverter());
        converters.add(new BooleanResultMessageConverter());
        super.configureMessageConverters(converters);
    }
}
