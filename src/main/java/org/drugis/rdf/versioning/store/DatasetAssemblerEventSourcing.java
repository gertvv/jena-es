package org.drugis.rdf.versioning.store;
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
	public static final Property PROPERTY_SOURCE = ResourceFactory.createProperty(EventSource.ES, "source");
	public static final Property PROPERTY_LOG = ResourceFactory.createProperty(EventSource.ES, "log");
	public static final Property PROPERTY_PREFIX = ResourceFactory.createProperty(EventSource.ES, "uriPrefix");

	@Override
	public Dataset createDataset(Assembler a, Resource root, Mode mode) {
		if (!exactlyOneProperty(root, PROPERTY_SOURCE)) {
			throw new AssemblerException(root, "Event sourcing dataset needs an event source");
		}
		if (!exactlyOneProperty(root, PROPERTY_LOG)) {
			throw new AssemblerException(root, "Event sourcing dataset needs a log");
		}
		if (!exactlyOneProperty(root, PROPERTY_PREFIX)) {
			throw new AssemblerException(root, "Event sourcing dataset needs a URI prefix");
		}

		Resource log = getRequiredResource(root, PROPERTY_LOG);
		String prefix = getRequiredLiteral(root, PROPERTY_PREFIX).getString();
		
		if (!log.getURI().startsWith(prefix + "datasets/")) {
			throw new AssemblerException(root, "The log URI must start with the URI prefix (+ datasets/)");
		}
		
		DatasetGraph dataStore = ((Dataset) a.open(a, getRequiredResource(root, PROPERTY_SOURCE), mode)).asDatasetGraph();
		
		EventSource eventSource = new EventSource(dataStore, prefix);
		eventSource.createDatasetIfNotExists(log.asNode()); // Automatically create log if needed
		DatasetGraph dsg = new DatasetGraphEventSourcing(eventSource, log.asNode());

        AssemblerUtils.setContext(root, dsg.getContext());
        return DatasetFactory.create(dsg) ;
	}
}
