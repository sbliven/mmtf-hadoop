package org.rcsb.mmtf.hadoop;

import java.util.ArrayList;
import java.util.List;

import org.biojava.nbio.structure.AminoAcidImpl;
import org.biojava.nbio.structure.Atom;
import org.biojava.nbio.structure.AtomImpl;
import org.biojava.nbio.structure.Chain;
import org.biojava.nbio.structure.ChainImpl;
import org.biojava.nbio.structure.Group;
import org.biojava.nbio.structure.contact.AtomContactSet;
import org.biojava.nbio.structure.contact.Grid;
import org.rcsb.mmtf.api.StructureDataInterface;

/**
 * A class of utils functions for finding charges.
 * @author Anthony Bradley
 *
 */
public class ChargeUtils {

	/**
	 * Get all the charged atoms in the structure using a {@link StructureDataInterface}.
	 * @param structure the input {@link StructureDataInterface}
	 * @return the list of charged atoms
	 */
	public static List<Atom> getChargedAtoms(StructureDataInterface structure) {
		List<Atom> atomList = new ArrayList<>();
		int lastNumGroup = 0;
		int atomCounter = 0;
		for(int chainInd=0; chainInd<structure.getChainsPerModel()[0]; chainInd++){
			int numGroups = structure.getGroupsPerChain()[chainInd];
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
			lastNumGroup+=structure.getGroupsPerChain()[chainInd];
		}
		return atomList;
	}
	
	/**
	 * Get all the atom contacts in a list of atoms.
	 * @param atoms the list of {@link Atom}s
	 * @param cutoff the cutoff distance
	 * @return the {@link AtomContactSet} of the contacts
	 */
	public static AtomContactSet getAtomContacts(List<Atom> atoms, double cutoff) {
		Grid grid = new Grid(cutoff);
		Atom[] atomArray = atoms.toArray(new Atom[atoms.size()]);
		grid.addAtoms(atomArray);
		return grid.getContacts();
	}
}
