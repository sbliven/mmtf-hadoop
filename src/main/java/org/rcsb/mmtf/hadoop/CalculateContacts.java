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
import org.biojava.nbio.structure.contact.Grid;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * Class to calculate all interatomic distances.
 * {@link Tuple2}{@link String}{@link StructureDataInterface} is the entry type.
 * {@link String} is the return type.
 * Flatmap means that the return from call must be an interable of String and Float (stored in Tuple2).
 * String and Float can be changed to any type (including custom data objects).
 * @author Anthony Bradley
 *
 */
public class CalculateContacts implements FlatMapFunction<Tuple2<String,StructureDataInterface>, String>{

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
	public Iterable<String> call(Tuple2<String, StructureDataInterface> t) throws Exception {
		// Get the pdb Id and the structure to loop through
		String pdbId = t._1;
		StructureDataInterface structure = t._2;
		// The list to return all the results in it must match Iterable<Tuple2<String, Float>> (return type of call) and String,
		List<String> outList = getDist(structure, pdbId, cutoff);
		return outList;
	}

	/**
	 * Example method of getting interatomic distances.
	 * This can be your generic function and be plugged above.
	 * @param structure the input structure to calculate from
	 * @return the list of {@link AtomContact} objects
	 */
	private List<String> getDist(StructureDataInterface structure, String pdbCode, double cutoff) {
		List<String> outList  = new ArrayList<>();
		int lastNumGroup = 0;
		atomCounter = 0;

		List<Atom> atomList = new ArrayList<>();
		for(int i=0; i<structure.getChainsPerModel()[0]; i++){
			atomList.addAll(getChargedAtoms(structure, i, lastNumGroup));
			lastNumGroup+=structure.getGroupsPerChain()[i];
		}
		if(atomList.size()>0){
			Grid grid = new Grid(cutoff);
			Atom[] atomArray = atomList.toArray(new Atom[atomList.size()]);
			grid.addAtoms(atomArray);
			for(AtomContact atomContact : grid.getContacts()){
				// Maybe add a filter here to ensure they're not 
				// in the same group
				Atom atomOne = atomContact.getPair().getFirst();
				Atom atomTwo = atomContact.getPair().getSecond();
				double distance = atomContact.getDistance();
				if(!atomOne.getGroup().getResidueNumber().getSeqNum().equals(atomTwo.getGroup().getResidueNumber().getSeqNum())){
					// This is how we write out each line in the file
					outList.add(writeLine(pdbCode, atomOne.getPDBserial(), atomOne.getCharge(), atomTwo.getPDBserial(), atomOne.getCharge(), distance));
				}
			}
		}
		return outList;
	}

	/**
	 * Function to write out each line of the total data file for the interactions.
	 * @param pdbCode the pdb code of the entry
	 * @param atomIdOne the serial id of atom one
	 * @param chargeOne the charge on atom one
	 * @param atomIdTwo the serial id of atom two
	 * @param chargeTwo the charge on atom two
	 * @param distance the distance between the two atoms in Angstromss
	 * @return the formatted string
	 */
	private String writeLine(String pdbCode, int atomIdOne, short chargeOne, int atomIdTwo, short chargeTwo,
			double distance) {
		// Very simple comma delimited string
		return pdbCode+","+atomIdOne+","+chargeOne+","+atomIdTwo+","+chargeTwo+","+distance;
	}

	/**
	 * Get the Charged atoms from the whole structure.
	 * @param structure the {@link StructureDataInterface} input
	 * @param chainInd the index of the chain
	 * @param lastNumGroup the index of the first group in the chain
	 * @return a list of C-alpha atoms
	 */
	private List<Atom> getChargedAtoms(StructureDataInterface structure, int chainInd, int lastNumGroup) {
		int numGroups = structure.getGroupsPerChain()[chainInd];
		List<Atom> atomList = new ArrayList<>();
		Chain chain = new ChainImpl();
		chain.setChainID(structure.getChainIds()[chainInd]);
		// Loop through the groups
		for(int i=0; i<numGroups; i++) {
			Group group = new AminoAcidImpl();
			group.setResidueNumber(structure.getChainIds()[chainInd], i, '?');
			group.setChain(chain);
			int groupType = structure.getGroupTypeIndices()[i+lastNumGroup];
			int[] atomCharges = structure.getGroupAtomCharges(groupType);
			for(int j=0; j<atomCharges.length; j++){
				if(atomCharges[j]!=0) {
					Atom atom = new AtomImpl();
					atom.setX(structure.getxCoords()[atomCounter]);
					atom.setY(structure.getyCoords()[atomCounter]);
					atom.setZ(structure.getzCoords()[atomCounter]);
					atom.setCharge((short) atomCharges[j]);
					atom.setPDBserial(structure.getAtomIds()[atomCounter]);
					atom.setGroup(group);
					atomList.add(atom);
				}
				atomCounter++;
			}
		}
		// Return the list
		return atomList;
	}
}
