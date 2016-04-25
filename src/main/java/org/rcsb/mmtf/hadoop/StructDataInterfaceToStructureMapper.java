package org.rcsb.mmtf.hadoop;

import org.apache.spark.api.java.function.PairFunction;
import org.biojava.nbio.structure.Structure;
import org.biojava.nbio.structure.io.mmtf.MmtfStructureReader;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.decoder.StructureDataToAdapter;

import scala.Tuple2;

/**
 * Class to map a {@link StructureDataInterface} onto a Biojava {@link Structure}.
 * @author Anthony Bradley
 *
 */
public class StructDataInterfaceToStructureMapper implements PairFunction<Tuple2<String, StructureDataInterface>,String, Structure> {

	/**
	 * Serial ID for this version of the class.
	 */
	private static final long serialVersionUID = -565493880259293921L;

	@Override
	public Tuple2<String, Structure> call(Tuple2<String, StructureDataInterface> t) throws Exception {
		MmtfStructureReader mmtfStructureReader = new MmtfStructureReader();
		new StructureDataToAdapter(t._2, mmtfStructureReader);
		return new Tuple2<String, Structure>(t._1,mmtfStructureReader.getStructure());
	}

}
