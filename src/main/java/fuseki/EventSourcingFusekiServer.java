package fuseki;
import javax.servlet.http.HttpServlet;

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
		String datasetPath = dsDesc.name;

		if (dsDesc.dataset instanceof DatasetGraphEventSourcing) {
			System.err.println("Configuring event sourced dataset");
			HttpServlet sparqlQuery = new EventSourced_QueryDataset();
			addServlet(context, "/" + datasetPath, sparqlQuery, dsDesc.query, enableCompression);
		}
		
	}
}