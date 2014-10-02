import static com.hp.hpl.jena.sparql.util.graph.GraphUtils.exactlyOneProperty;

import com.hp.hpl.jena.assembler.Assembler;
import com.hp.hpl.jena.assembler.Mode;
import com.hp.hpl.jena.assembler.exceptions.AssemblerException;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.assembler.AssemblerUtils;
import com.hp.hpl.jena.sparql.core.assembler.DatasetAssembler;


public class DatasetAssemblerEventSourcing extends DatasetAssembler {
	public static final Property PROPERTY_SOURCE = ResourceFactory.createProperty(EventSource2.ES, "source");
	public static final Property PROPERTY_LOG = ResourceFactory.createProperty(EventSource2.ES, "log");

	@Override
	public Dataset createDataset(Assembler a, Resource root, Mode mode) {
		if (!exactlyOneProperty(root, PROPERTY_SOURCE)) {
			throw new AssemblerException(root, "Event sourcing dataset needs an event source");
		}
		if (!exactlyOneProperty(root, PROPERTY_LOG)) {
			throw new AssemblerException(root, "Event sourcing dataset needs a log");
		}
		
		DatasetGraph eventSource = ((Dataset) a.open(a, getRequiredResource(root, PROPERTY_SOURCE), mode)).asDatasetGraph();
		Resource log = getRequiredResource(root, PROPERTY_LOG);
		DatasetGraph dsg = new DatasetGraphEventSourcing(eventSource, log.asNode());

        AssemblerUtils.setContext(root, dsg.getContext());
        return DatasetFactory.create(dsg) ;
	}
}
