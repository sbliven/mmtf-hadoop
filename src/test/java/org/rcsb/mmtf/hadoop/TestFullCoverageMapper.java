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
 * Created on Apr 29, 2016
 * Author: blivens 
 *
 */
 
package org.rcsb.mmtf.hadoop;

import static org.junit.Assert.*;

import java.nio.channels.InterruptedByTimeoutException;

import org.junit.Test;

public class TestFullCoverageMapper {

	@Test
	public void testFitStoichiometry() throws InterruptedByTimeoutException {
		int[] uc, comp;
		boolean result;
		int[][] assemblies = new int[][] {
				new int[] { 2, 1, 1 },
				new int[] { 1, 2, 0 },
				new int[] { 0, 0, 1 },
				new int[] { 1, 1, 1 },
		};
		
		uc = new int[] { 2, 1, 1 };
		comp = new int[assemblies.length];
		result = FullCoverageMapper.fitStoichiometry(uc, assemblies, comp);
		assertTrue(result);
		assertArrayEquals(new int[] { 2, 1, 1 }, uc);
		assertArrayEquals(new int[] { 1, 0,0,0 }, comp);
		
		uc = new int[] { 0,0, 4 };
		comp = new int[assemblies.length];
		result = FullCoverageMapper.fitStoichiometry(uc, assemblies, comp);
		assertTrue(result);
		assertArrayEquals(new int[] { 0,0,4 }, uc);
		assertArrayEquals(new int[] { 0, 0,4,0 }, comp);
		
		uc = new int[] { 4,4,4 };
		comp = new int[assemblies.length];
		result = FullCoverageMapper.fitStoichiometry(uc, assemblies, comp);
		assertTrue(result);
		assertArrayEquals(new int[] { 4,4,4 }, uc);
		assertArrayEquals(new int[] { 1,1,2,1 }, comp);
		
		uc = new int[] { 4,2,1 };
		comp = new int[assemblies.length];
		result = FullCoverageMapper.fitStoichiometry(uc, assemblies, comp);
		assertFalse(result);
		assertArrayEquals(new int[] { 4,2,1 }, uc);
		assertArrayEquals(new int[] { 0,0,0,0 }, comp);

		uc = new int[] { 0,0,0 };
		comp = new int[assemblies.length];
		result = FullCoverageMapper.fitStoichiometry(uc, assemblies, comp);
		assertTrue(result);
		assertArrayEquals(new int[] { 0,0,0 }, uc);
		assertArrayEquals(new int[] { 0,0,0,0 }, comp);
		
		uc = new int[] { 0,0,-1 };
		comp = new int[assemblies.length];
		result = FullCoverageMapper.fitStoichiometry(uc, assemblies, comp);
		assertFalse(result);
		assertArrayEquals(new int[] { 0,0,-1 }, uc);
		assertArrayEquals(new int[] { 0,0,0,0 }, comp);

	}

}
