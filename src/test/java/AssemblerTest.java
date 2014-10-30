import java.util.Iterator;

import org.apache.jena.riot.RDFDataMgr;

import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.sparql.core.assembler.AssemblerUtils;
import com.hp.hpl.jena.tdb.assembler.VocabTDB;


public class AssemblerTest {

	public static void main(String[] args) {
		// Load the initial data
		Dataset data = RDFDataMgr.loadDataset("data.trig");

		// Open the source dataset and overwrite its content
		Dataset datasetTDB = (Dataset) AssemblerUtils.build("assemble-es.ttl", VocabTDB.tDatasetTDB);
		datasetTDB.asDatasetGraph().clear();
		for (Iterator<String> it = data.listNames(); it.hasNext(); ) {
			String graph = it.next();
			datasetTDB.addNamedModel(graph, data.getNamedModel(graph));
		}
		datasetTDB.close();
		
		Dataset datasetES = (Dataset) AssemblerUtils.build("assemble-es.ttl", "http://drugis.org/eventSourcing/es#EventSourcedDataset");
		
		datasetES.begin(ReadWrite.READ);
		System.out.println(datasetES.asDatasetGraph().getGraph(NodeFactory.createURI("http://example.com/PeterParker")));
		datasetES.end();
		
		datasetES.close();
	}
}
