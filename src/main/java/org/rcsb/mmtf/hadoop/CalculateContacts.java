package org.rcsb.mmtf.hadoop;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.biojava.nbio.structure.AminoAcidImpl;
import org.biojava.nbio.structure.Atom;
import org.biojava.nbio.structure.AtomImpl;
import org.biojava.nbio.structure.Chain;
import org.biojava.nbio.structure.ChainImpl;
import org.biojava.nbio.structure.Group;
import org.biojava.nbio.structure.contact.AtomContact;
import org.biojava.nbio.structure.contact.AtomContactSet;
import org.biojava.nbio.structure.contact.Grid;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * Class to calculate all interatomic distances.
 * {@link Tuple2}{@link String}{@link StructureDataInterface} is the entry type.
 * {@link String}{@link Float} are the return types.
 * Flatmap means that the return from call must be an interable of String and Float (stored in Tuple2).
 * String and Float can be changed to any type (including custom data objects).
 * @author Anthony Bradley
 *
 */
public class CalculateContacts implements FlatMapFunction<Tuple2<String,StructureDataInterface>, AtomContact>{

	private double cutoff;
	private int atomCounter;

	/**
	 * @param cutoff
	 */
	public CalculateContacts(double cutoff) {
		this.cutoff = cutoff;
	}


	/**
	 * This is required because this class implements {@link Serializable}.
	 */
	private static final long serialVersionUID = 7102351722106317536L;

	@Override
	public Iterable<AtomContact> call(Tuple2<String, StructureDataInterface> t) throws Exception {
		// Get the pdb Id and the structure to loop through
		String pdbId = t._1;
		StructureDataInterface structure = t._2;
		// The list to return all the results in it must match Iterable<Tuple2<String, Float>> (return type of call) and String,
		List<AtomContact> outList = getDist(structure, pdbId, cutoff);
		return outList;
	}

	/**
	 * Example method of getting interatomic distances.
	 * This can be your generic function and be plugged above.
	 * @param structure the input structure to calculate from
	 * @return the list of {@link AtomContact} objects
	 */
	private List<AtomContact> getDist(StructureDataInterface structure, String pdbCode, double cutoff) {
		List<AtomContact> outList  = new ArrayList<>();
		int lastNumGroup = 0;
		atomCounter = 0;
		for(int i=0; i<structure.getChainsPerModel()[0]; i++){
			Grid grid = new Grid(cutoff);
			Atom[] atomList = getChargedAtoms(structure,i, lastNumGroup,atomCounter);
			if(atomList.length>0){
				grid.addAtoms(atomList);
				AtomContactSet contacts =  grid.getContacts();
				outList.addAll(contacts.getContactsWithinDistance(cutoff-0.000001));
			}
			lastNumGroup+=structure.getGroupsPerChain()[i];
		}
		return outList;
	}

	/**
	 * Get the C-alpha atoms from the chain.
	 * @param structure the {@link StructureDataInterface} input
	 * @param chainInd the index of the chain
	 * @param lastNumGroup the index of the first group in the chain
	 * @return a list of C-alpha atoms
	 */
	private Atom[] getChargedAtoms(StructureDataInterface structure, int chainInd, int lastNumGroup, int atomCounter) {
		int numGroups = structure.getGroupsPerChain()[chainInd];
		List<Atom> atomList = new ArrayList<>();
		Group group = new AminoAcidImpl();
		Chain chain = new ChainImpl();
		chain.setChainID(structure.getChainIds()[chainInd]);
		group.setChain(chain);
		// Loop through the groups
		for(int i=0; i<numGroups; i++) {
			int groupType = structure.getGroupTypeIndices()[i+lastNumGroup];
			int[] atomCharges = structure.getGroupAtomCharges(groupType);
			for(int j=0; j<atomCharges.length; j++){
				if(atomCharges[j]!=0) {
					Atom atom = new AtomImpl();
					atom.setX(structure.getxCoords()[atomCounter]);
					atom.setY(structure.getyCoords()[atomCounter]);
					atom.setZ(structure.getzCoords()[atomCounter]);
					atom.setPDBserial(structure.getAtomIds()[atomCounter]);
					atom.setGroup(group);
					atomList.add(atom);
				}
				atomCounter++;
			}
		}
		// Return the list
		return atomList.toArray(new Atom[0]);
	}
}
