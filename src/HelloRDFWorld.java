import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.jena.fuseki.EmbeddedFusekiServer;
import org.apache.jena.fuseki.Fuseki;
import org.apache.jena.fuseki.HttpNames;
import org.apache.jena.fuseki.server.DatasetRef;
import org.apache.jena.fuseki.server.SPARQLServer;
import org.apache.jena.fuseki.server.ServerConfig;
import org.apache.jena.fuseki.server.ServiceRef;
import org.apache.jena.fuseki.servlets.SPARQL_Query;
import org.apache.jena.fuseki.servlets.SPARQL_QueryDataset;
import org.eclipse.jetty.servlet.ServletContextHandler;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.ParameterizedSparqlString;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.sparql.core.assembler.AssemblerUtils;
import com.hp.hpl.jena.sparql.core.assembler.DatasetAssemblerVocab;

public class HelloRDFWorld {

	static String readFile(String path, Charset encoding) 
			throws IOException {
		byte[] encoded = Files.readAllBytes(Paths.get(path));
		return new String(encoded, encoding);
	}

	public static void main(String[] args) {
		
		Dataset datasetInternal = (Dataset) AssemblerUtils.build("assemble.ttl", DatasetAssemblerVocab.tDataset);
//		dataset.getNamedModel(spec.expandPrefix("test:meta")).write(System.out, "Turtle");
//		Dataset dataset = DatasetImpl.wrap(new EventSourcingDatasetGraph(datasetInternal.asDatasetGraph()));
		String a = "http://test.drugis.org/person/Gert";
		String t = "2006-01-23T00:00:00";
		Dataset dataset = EventSourced_QueryDataset.createEventSourcedDataset(datasetInternal, a, t);
		
//		dataset.getNamedModel("http://test.drugis.org/person/Gert").write(System.out, "Turtle");
		
		String testQueryString = "PREFIX foaf: <http://xmlns.com/foaf/0.1/> PREFIX person: <http://test.drugis.org/person/> SELECT ?mbox ?url WHERE { GRAPH person:Gert { person:Gert foaf:homepage ?url; foaf:mbox ?mbox . } }";
		Query query = QueryFactory.create(testQueryString);
		System.out.println("Response at " + t);
		queryOut(query, dataset);
		
		t = "2009-04-01T00:00:00";
		dataset = EventSourced_QueryDataset.createEventSourcedDataset(datasetInternal, a, t);
		System.out.println("Response at " + t);
		queryOut(query, dataset);
		
//		EmbeddedFusekiServer server = EmbeddedFusekiServer.create(3030, dataset.asDatasetGraph(), "/comp");
		//server.start();
		// http://localhost:3030/comp/query?query=PREFIX%20foaf:%20%3Chttp://xmlns.com/foaf/0.1/%3E%20PREFIX%20person:%20%3Chttp://test.drugis.org/person/%3E%20SELECT%20?mbox%20?url%20WHERE%20{%20GRAPH%20person:Gert%20{%20person:Gert%20foaf:homepage%20?url;%20foaf:mbox%20?mbox%20.%20}%20}
		
		EventSourcingDatasetRef ref = new EventSourcingDatasetRef();
		String datasetPath = "/ds";
		ref.name = datasetPath;
		ref.dataset = datasetInternal.asDatasetGraph();
		ref.query.endpoints.add(HttpNames.ServiceQuery);
		ref.query.endpoints.add(HttpNames.ServiceQueryAlt);
		ref.readGraphStore.endpoints.add(HttpNames.ServiceData);
		ref.history.endpoints.add("history");

		ServerConfig config = new ServerConfig();
		config.datasets = Arrays.asList(new DatasetRef[] { ref });
		config.port = 3030;
		//SPARQLServer server = new SPARQLServer(config);
		EventSourcingSPARQLServer server = new EventSourcingSPARQLServer(config);
		server.start();
		
		requestCSV("http://localhost:3030/ds/query", testQueryString, "2009-04-01T00:00:00");
		requestCSV("http://localhost:3030/ds/query", testQueryString, "2006-01-23T00:00:00");
// http://localhost:3030/ds/query?t=2006-01-23T00:00:00&query=PREFIX%20foaf:%20%3Chttp://xmlns.com/foaf/0.1/%3E%20PREFIX%20person:%20%3Chttp://test.drugis.org/person/%3E%20SELECT%20?mbox%20?url%20WHERE%20{%20GRAPH%20person:Gert%20{%20person:Gert%20foaf:homepage%20?url;%20foaf:mbox%20?mbox%20.%20}%20}		
// http://localhost:3030/ds/query?t=2009-04-01T00:00:00&query=PREFIX%20foaf:%20%3Chttp://xmlns.com/foaf/0.1/%3E%20PREFIX%20person:%20%3Chttp://test.drugis.org/person/%3E%20SELECT%20?mbox%20?url%20WHERE%20{%20GRAPH%20person:Gert%20{%20person:Gert%20foaf:homepage%20?url;%20foaf:mbox%20?mbox%20.%20}%20}
	}

	private static void requestCSV(String url, String query, String t) {
		String charset = "UTF-8";
		try {
			String queryStr = String.format("t=%s&query=%s", 
				     URLEncoder.encode(t, charset), 
				     URLEncoder.encode(query, charset));
			URLConnection connection = new URL(url + "?" + queryStr).openConnection();
			connection.setRequestProperty("Accept-Charset", charset);
			connection.setRequestProperty("Accept", "text/csv");
			
			InputStream response = connection.getInputStream();
			
			String contentType = connection.getHeaderField("Content-Type");
			charset = null;

			for (String param : contentType.replace(" ", "").split(";")) {
			    if (param.startsWith("charset=")) {
			        charset = param.split("=", 2)[1];
			        break;
			    }
			}

			if (charset != null) {
			    try (BufferedReader reader = new BufferedReader(new InputStreamReader(response, charset))) {
			        for (String line; (line = reader.readLine()) != null;) {
			            System.out.println(line);
			        }
			    }
			}
		} catch (UnsupportedEncodingException e) {
			
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void queryOut(Query query, Dataset dataset) {
		try (QueryExecution qe = QueryExecutionFactory.create(query, dataset)) {
			ResultSet results = qe.execSelect();
			ResultSetFormatter.out(System.out, results);
		}
	}
}
