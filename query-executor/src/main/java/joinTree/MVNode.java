package joinTree;

import java.util.List;

/**
 * Abstract class that represent a node which contains a list of triples. For
 * example, if there exist multiple triples with the same subject they are
 * unioned in one node and property table is queried.
 *
 * @author Polina Koleva
 */
public abstract class MVNode extends Node {

	public List<TriplePattern> tripleGroup;

	@Override
	public List<TriplePattern> collectTriples() {
		return tripleGroup;
	}

}