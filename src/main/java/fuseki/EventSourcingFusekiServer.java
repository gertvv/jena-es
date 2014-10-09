package fuseki;
import org.apache.jena.fuseki.server.DatasetRef;
import org.apache.jena.fuseki.server.ServerConfig;
import org.eclipse.jetty.servlet.ServletContextHandler;

import es.DatasetGraphEventSourcing;


public class EventSourcingFusekiServer extends SPARQLServer {

	public EventSourcingFusekiServer(ServerConfig config) {
		super(config);
	}
	
	@Override
	protected void configureOneDataset(ServletContextHandler context, DatasetRef dsDesc, boolean enableCompression) {
		super.configureOneDataset(context, dsDesc, enableCompression);
		
		if (dsDesc.dataset instanceof DatasetGraphEventSourcing) {
			System.err.println("Configuring event sourced dataset");
		}
		
	}
}