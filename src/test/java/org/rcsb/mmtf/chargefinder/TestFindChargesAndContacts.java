package org.rcsb.mmtf.chargefinder;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.biojava.nbio.structure.Atom;
import org.biojava.nbio.structure.Bond;
import org.biojava.nbio.structure.BondImpl;
import org.biojava.nbio.structure.Chain;
import org.biojava.nbio.structure.Group;
import org.biojava.nbio.structure.Structure;
import org.biojava.nbio.structure.StructureException;
import org.biojava.nbio.structure.StructureIO;
import org.biojava.nbio.structure.align.util.AtomCache;
import org.biojava.nbio.structure.contact.AtomContact;
import org.biojava.nbio.structure.contact.AtomContactSet;
import org.biojava.nbio.structure.io.ChargeAdder;
import org.junit.Test;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.dataholders.MmtfStructure;
import org.rcsb.mmtf.decoder.DefaultDecoder;
import org.rcsb.mmtf.decoder.ReaderUtils;
import org.rcsb.mmtf.hadoop.ChargeUtils;


/**
 * Calss to test finding charges and finding contacts between charges.
 * @author Anthony Bradley
 *
 */
public class TestFindChargesAndContacts {

	/**
	 * Test a few examples to check the behaviour is the same.
	 * @throws StructureException 
	 * @throws IOException 
	 */
	@Test
	public void testAll() throws IOException, StructureException {
		
		// Multi model structure
		testCompareBiojava("1cdr", 5.0);
		// Another weird structure (jose's suggestion) 
		testCompareBiojava("3zyb", 5.0);
		//Standard structure
		testCompareBiojava("4cup", 5.0);
		// Weird NMR structure
		testCompareBiojava("1o2f", 5.0);
		// B-DNA structure
		testCompareBiojava("1bna", 5.0); 
		// DNA structure
		testCompareBiojava("4y60", 5.0);
	}
	
	/**
	 * Compare Biojava {@link Structure} and {@link StructureDataInterface} to check 
	 * they return the same values.
	 * @throws IOException
	 * @throws StructureException
	 */
	public void testCompareBiojava(String pdbId, double cutoff) throws IOException, StructureException {

		
		
		// Get the list of charged atoms using both methods
		List<Atom> biojavaChargedAtoms = getBiojavaData(pdbId);
		List<Atom> structDataIntChargedAtoms = getStructDataData(pdbId);
		assertEquals(biojavaChargedAtoms.size(), structDataIntChargedAtoms.size());

		if (biojavaChargedAtoms.size()==0){
			System.out.println("No charged atoms in: "+pdbId);
			return;
		}
		// Now get the contacts using the simple method
		int biojavaNumContactsSimple = getAtomContactsSimple(biojavaChargedAtoms, cutoff);
		int structureNumContactsSimple = getAtomContactsSimple(structDataIntChargedAtoms, cutoff);
		assertEquals(biojavaNumContactsSimple, structureNumContactsSimple);
		
		// Now check using the grid method
		int biojavaNumContactsGrid = filterForLikeGroup(ChargeUtils.getAtomContacts(biojavaChargedAtoms, cutoff));
		int structureNumContactsGrid = filterForLikeGroup(ChargeUtils.getAtomContacts(structDataIntChargedAtoms, cutoff));
		assertEquals(biojavaNumContactsGrid, structureNumContactsGrid);

		// Now check they are the same
		assertEquals(biojavaNumContactsGrid, biojavaNumContactsSimple);

	}

	private int filterForLikeGroup(AtomContactSet atomContactSet) {
		int contacts = 0;
		for(AtomContact atomContact : atomContactSet){
			// Maybe add a filter here to ensure they're not 
			// in the same group
			Atom atomOne = atomContact.getPair().getFirst();
			Atom atomTwo = atomContact.getPair().getSecond();
			if(!atomOne.getGroup().getResidueNumber().getSeqNum().equals(atomTwo.getGroup().getResidueNumber().getSeqNum())){
				// This is how we write out each line in the file
				contacts++;
			}
		}
		return contacts;
	}

	/**
	 * Get the the charged atoms using {@link StructureDataInterface}
	 * @param pdbId the input pdb id
	 * @return the list of charged atoms
	 * @throws IOException
	 */
	private List<Atom> getStructDataData(String pdbId) throws IOException {
		MmtfStructure mmtfStructure = ReaderUtils.getDataFromUrl(pdbId);
		StructureDataInterface structureDataInterface = new DefaultDecoder(mmtfStructure);
		return ChargeUtils.getChargedAtoms(structureDataInterface);
	}

	/**
	 * Get the charged atoms using Biojava.
	 * @param pdbId the PDB code to find
	 * @return the list of charged atoms
	 * @throws IOException
	 * @throws StructureException
	 */
	private List<Atom> getBiojavaData(String pdbId) throws IOException, StructureException {
		AtomCache atomCache = new AtomCache();
		atomCache.setUseMmtf(false);
		atomCache.setUseMmCif(true);
		StructureIO.setAtomCache(atomCache);
		Structure structure = StructureIO.getStructure(pdbId);
		ChargeAdder.addCharges(structure);
		List<Atom> atoms = new ArrayList<>();
		for(Chain chain : structure.getChains()) {
			for (Group group : chain.getAtomGroups()) {
				for (Atom atom : group.getAtoms()) {
					if (atom.getCharge()!=0) {
						atoms.add(atom);
					}
				}
				for (Group altLoc : group.getAltLocs()) {
					for (Atom atom : altLoc.getAtoms()) {
						if (atom.getCharge()!=0) {
							atoms.add(atom);
						}
					}
				}
			}
		}
		return atoms;
	}
	
	
	
	
	/**
	 * A simple method for finding atom contacts for a single structure.
	 * @param atoms the input list of Biojava {@link Atom}s
	 * @param threshold the threshold for finding an interaction
	 * @return the number of contacts
	 */
	private int getAtomContactsSimple(List<Atom> atoms, double threshold) {
		int atomInts = 0;
		for(int i=0; i<atoms.size(); i++) {
			for(int j=i; j<atoms.size(); j++) {
				Atom atomOne = atoms.get(i);
				Atom atomTwo = atoms.get(j);
				if(!atomOne.getGroup().getResidueNumber().equals(atomTwo.getGroup().getResidueNumber())){
					Bond bond = new BondImpl(atomOne, atomTwo, 1);
					double dist = bond.getLength();
					if(dist<=threshold){
						atomInts++;
					}
				}

			}
		}
		return atomInts;
	}

}
