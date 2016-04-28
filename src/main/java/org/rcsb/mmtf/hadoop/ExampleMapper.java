package org.rcsb.mmtf.hadoop;

import org.apache.spark.api.java.function.PairFunction;
import org.biojava.nbio.structure.Chain;
import org.biojava.nbio.structure.Group;
import org.biojava.nbio.structure.Structure;

import scala.Tuple2;

/**
 * Example function for counting all the atoms (not including alt locs) in a structure.
 * @author Anthony Bradley
 *
 */
public class ExampleMapper implements PairFunction<Tuple2<String, Structure>,String, Integer>  {

	/**
	 * Serial id for this instance of the class.
	 */
	private static final long serialVersionUID = 7191372270559563321L;

	@Override
	public Tuple2<String, Integer> call(Tuple2<String, Structure> t) throws Exception {
		Structure structure = t._2;
		int counter = 0;
		for (int i=0; i<structure.nrModels(); i++) {
			for (Chain chain : structure.getChains(i)){
				for (Group group : chain.getAtomGroups()) {
					counter+=group.getAtoms().size();	
				}
			}
		}
		// Now return this answer
		return new Tuple2<String, Integer>(t._1, counter);
	}
}
