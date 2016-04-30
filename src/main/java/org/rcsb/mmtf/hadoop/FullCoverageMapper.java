/*
 *                    BioJava development code
 *
 * This code may be freely distributed and modified under the
 * terms of the GNU Lesser General Public Licence.  This should
 * be distributed with the code.  If you do not have a copy,
 * see:
 *
 *      http://www.gnu.org/copyleft/lesser.html
 *
 * Copyright for this code is held jointly by the individual
 * authors.  These should be listed in @author doc comments.
 *
 * For more information on the BioJava project and its aims,
 * or to join the biojava-l mailing list, visit the home page
 * at:
 *
 *      http://www.biojava.org/
 *
 * Created on Apr 27, 2016
 * Author: blivens 
 *
 */
 
package org.rcsb.mmtf.hadoop;

import java.nio.channels.InterruptedByTimeoutException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.api.java.function.PairFunction;
import org.biojava.nbio.structure.xtal.SpaceGroup;
import org.rcsb.mmtf.api.StructureDataInterface;

import scala.Tuple2;

/**
 * Analyze an input structure and return a string representing its stoichiometry
 * and coverage properties.
 * @author Spencer Bliven
 *
 */
public class FullCoverageMapper implements PairFunction<Tuple2<String, StructureDataInterface>,String, String> {

	private static final long serialVersionUID = 3812703935579649962L;

	@Override
	public Tuple2<String, String> call(
			Tuple2<String, StructureDataInterface> t) throws Exception {
		StringBuffer str = new StringBuffer();
		long startTime = System.currentTimeMillis();
		StructureDataInterface data = t._2;
		
		int numChains = data.getChainsPerModel()[0];
		String[] chainIds = data.getChainIds();
		
		// Reduce chains to polymers
		int[] polymerInd = new int[chainIds.length];//index of polymer chains
		Arrays.fill(polymerInd,-1);
		int polyChains=0;
		for(int entityInd=0;entityInd < data.getNumEntities(); entityInd++) {
			String type = data.getEntityType(entityInd);
			if( type.equalsIgnoreCase("polymer")) {
				for(int chainInd : data.getEntityChainIndexList(entityInd) ) {
					if(chainInd<numChains) {// in model 0
						polymerInd[chainInd] = polyChains;
						polyChains++;
					}
				}
			}
		}

		// Only way to get the number of ops is to construct the full Structure?
//		MmtfStructureReader mmtfStructureReader = new MmtfStructureReader();
//		new StructureDataToAdapter(t._2, mmtfStructureReader);
//		Structure struct = mmtfStructureReader.getStructure();
//		SpaceGroup sg = struct.getCrystallographicInfo().getSpaceGroup();
		SpaceGroup sg = SpaceGroup.parseSpaceGroup(data.getSpaceGroup());
		int ops;
		if(sg != null)
			// TODO doesn't include NCS operators (MMTF issue #2
			ops = sg.getNumOperators();
		else
			ops = 1;
		int[] ucstoich = new int[polyChains];
		Arrays.fill(ucstoich, ops);
		
		

		int[][] stoichiometries = new int[data.getNumBioassemblies()][];
		for(int bioassemblyIndex=0;bioassemblyIndex<data.getNumBioassemblies();bioassemblyIndex++) {
			int[] stoich = new int[polyChains];
			for(int transformationIndex=0;transformationIndex<data.getNumTransInBioassembly(bioassemblyIndex);transformationIndex++) {
				//double[] matVals = data.getMatrixForTransform(bioassemblyIndex, transformationIndex);
				int[] chainlist = data.getChainIndexListForTransform(bioassemblyIndex, transformationIndex);
				for(int chain : chainlist) {
					int polyInd = polymerInd[chain];
					if(polyInd >= 0)
					stoich[polyInd] ++;
				}
			}
			stoichiometries[bioassemblyIndex] = stoich;
		}

		// Append stoichiometries to output
		str.append(String.format("UC:%s\t",getStoichiometryString(chainIds, ucstoich)));
		str.append(IntStream.range(0,stoichiometries.length)
				.mapToObj(ba -> String.format("pdb%d:%s",ba+1,getStoichiometryString(chainIds, stoichiometries[ba])))
				.collect(Collectors.joining(";"))
				);


		// TODO determine whether the UC stoichiometry is consistent with the bioassemblies.
		// Let BAs have stoichiometries A = [a_ij] where a_ij gives the number of
		// copies of chain i in assembly j.
		// The unit cell contains b copies of each chain.
		// Have linear diophantine system A*x = B, where we want x (the number
		// of copies of each assembly required to cover the unit cell.
		// Can be solved by Smith normal form, but perhaps overkill?
		
		// Actually, for each assembly we want to know whether a solution exists
		// with at least one copy of that assembly
		
		try {
			int[][] fits = fitStoichiometryAll(ucstoich, stoichiometries);

			str.append("\tCovers: ");
			str.append( new HashSet<>(Arrays.asList(fits)).stream()
					.filter(fit -> fit != null)
					.map(fit -> IntStream.range(0,fit.length)
							.filter(i -> fit[i] > 0)
							.mapToObj(i -> String.format("pdb%d",i+1)+ (fit[i]>1 ? "_"+(i+1) : "" ) )
							.collect(Collectors.joining(","))
							)
							.collect(Collectors.joining("; ")) );

			String missing = IntStream.range(0, fits.length)
					.filter(i -> fits[i] == null)
					.mapToObj(i -> String.format("pdb%d", i+1))
					.collect(Collectors.joining(","));
			if( !missing.isEmpty() ) {
				str.append("\tNo ");
				str.append(missing);
			}
		} catch(InterruptedByTimeoutException e) {
			str.append("\tTimeout");
		}

		str.append("Time:");
		str.append(System.currentTimeMillis()-startTime);
		return new Tuple2<>(t._1, str.toString());
	}

	private String getStoichiometryString(String[] chainIds, int[] stoich) {
		return IntStream.range(0, stoich.length)
				.filter(i -> stoich[i] > 0)
				.mapToObj( i -> chainIds[i] + (stoich[i] > 1 ? "_"+stoich[i] : "") )
				.collect(Collectors.joining(" "));
	}

	public static int[][] fitStoichiometryAll(int[] uc, int[][] assemblies) throws InterruptedByTimeoutException {
		// check that lengths are consistent
		if( Arrays.asList(assemblies).stream().anyMatch(a -> uc.length != a.length) ) {
			throw new IndexOutOfBoundsException("Input lengths differ");
		}

		int[][] results = new int[assemblies.length][];

		// select an assembly to apply
		for(int i=0;i<assemblies.length;i++) {
			if(results[i] != null) {
				continue;
			}
			results[i] = new int[assemblies.length];
			// Apply the assembly
			sub(uc,assemblies[i]);
			results[i][i]++;
			// Check if successful forcing this assembly
			boolean success = fitStoichiometry(uc, assemblies, results[i]);
			// Back out
			add(uc,assemblies[i]);
			if(!success) {
				results[i] = null;
			} else {
				// mark subsequent included assemblies as successful
				for(int j=i+1;j<assemblies.length;j++) {
					if( results[i][j] > 0 ) {
						results[j] = results[i];
					}
				}
			}
		}
		return results;
	}
	/**
	 * Attempts to find a set of assemblies which completely cover the unit cell.
	 * If this is possible (return value of true), then the following holds for all j:
	 * 
	 *     <pre>composition[i]*assemblies[i][j] == uc[j]</pre>
	 *
	 * The method proceeds by brute force recursion.
	 * @param uc unit cell stoichiometry over n entities
	 * @param assemblies m*n array of stoichiometries of each assembly
	 * @param composition output array of length m with the number of copies of each assembly fit
	 * @return true if the current composition exactly covers the unit cell
	 * @throws InterruptedByTimeoutException 
	 */
	public static boolean fitStoichiometry(int[] uc, int[][] assemblies, int[] composition) throws InterruptedByTimeoutException {
		// check that lengths are consistent
		if( Arrays.asList(assemblies).stream().anyMatch(a -> uc.length != a.length)
				|| assemblies.length != composition.length ) {
			throw new IndexOutOfBoundsException("Input lengths differ");
		}
		long maxTime = System.currentTimeMillis() + 20000; // Shouldn't take more than 20 sec
		return fitStoichiometry(uc, assemblies, composition, assemblies.length,maxTime);
	}
	/**
	 * Helper version
	 * @param uc unit cell stoichiometry over n entities
	 * @param assemblies m*n array of stoichiometries of each assembly
	 * @param composition output array of length m with the number of copies of each assembly fit
	 * @param numAssemblies Only consider the first numAssemblies elements of assemblies.
	 * @return true if the current composition exactly covers the unit cell
	 * @throws InterruptedByTimeoutException 
	 */
	private static boolean fitStoichiometry(int[] uc, int[][] assemblies, int[] composition, int numAssemblies,long maxTime) throws InterruptedByTimeoutException {
		if(maxTime >0 && System.currentTimeMillis() > maxTime) {
			throw new InterruptedByTimeoutException();
		}
		// Check if we're done
		boolean done = true;
		for(int i=0;i<uc.length;i++) {
			if(uc[i] != 0) {
				done = false;
				break;
			}
		}
		if(done) {
			return true;
		}
		// select an assembly to apply
		for(int i=0;i<numAssemblies;i++) {
			if( lte(assemblies[i],uc) ) {
				// Apply the assembly
				sub(uc,assemblies[i]);
				composition[i]++;
				// Recurse
				boolean success = fitStoichiometry(uc, assemblies, composition, i+1,maxTime);
				// Back out
				add(uc,assemblies[i]);
				if(success) {
					return true;
				} else {
					composition[i]--;
				}
			}
		}
		// No assemblies were successful
		return false;
	}
	/**
	 * Subtracts b from a (in place)
	 */
	private static void sub(int[] a, int[] b ) {
		for(int i=0;i<a.length;i++)
			a[i]-=b[i];
	}
	/**
	 * Adds b to a (in place)
	 */
	private static void add(int[] a, int[] b ) {
		for(int i=0;i<a.length;i++)
			a[i]+=b[i];
	}
	/**
	 * @return true if a[i] <= b[i] for all i
	 */
	private static boolean lte(int[] a, int[] b) {
		for(int i=0;i<a.length;i++)
			if(a[i]>b[i])
				return false;
		return true;
	}
}
