package org.drugis.rdf.versioning.server;

import java.util.List;

import org.springframework.boot.autoconfigure.web.WebMvcAutoConfiguration.WebMvcAutoConfigurationAdapter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.HttpMessageConverter;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.tdb.TDBFactory;

import es.DatasetGraphEventSourcing;

@Configuration
public class Config extends WebMvcAutoConfigurationAdapter {

	@Bean
	public DatasetGraphEventSourcing dataset() {
		DatasetGraph eventSource = TDBFactory.createDatasetGraph("DB");
		Node logUri = NodeFactory.createURI("http://drugis.org/eventSourcing/es#log");
		return new DatasetGraphEventSourcing(eventSource, logUri);
	}
	
    @Override
    public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
        converters.add(new JenaGraphMessageConverter());
        converters.add(new JenaSPARQLResultConverter());
        super.configureMessageConverters(converters);
    }

}
