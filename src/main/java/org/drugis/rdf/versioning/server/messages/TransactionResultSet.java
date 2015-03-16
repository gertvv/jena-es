package org.drugis.rdf.versioning.server.messages;

import java.util.List;

import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.sparql.core.Transactional;
import com.hp.hpl.jena.sparql.engine.binding.Binding;

public class TransactionResultSet implements ResultSet {
	private ResultSet d_nested;
	private Transactional d_transactional;

	public TransactionResultSet(ResultSet nested, Transactional transactional) {
		d_nested = nested;
		d_transactional = transactional;
	}

	@Override
	public void remove() {
		d_nested.remove();
	}

	@Override
	public boolean hasNext() {
		return d_nested.hasNext();
	}

	@Override
	public QuerySolution next() {
		return d_nested.next();
	}

	@Override
	public QuerySolution nextSolution() {
		return d_nested.nextSolution();
	}

	@Override
	public Binding nextBinding() {
		return d_nested.nextBinding();
	}

	@Override
	public int getRowNumber() {
		return d_nested.getRowNumber();
	}

	@Override
	public List<String> getResultVars() {
		return d_nested.getResultVars();
	}

	@Override
	public Model getResourceModel() {
		return d_nested.getResourceModel();
	}
	
	public void endTransaction() {
		d_transactional.end();
	}
}
