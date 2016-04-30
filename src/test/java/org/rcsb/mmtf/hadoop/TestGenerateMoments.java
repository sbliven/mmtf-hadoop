package org.rcsb.mmtf.hadoop;

import java.awt.Point;

import javax.vecmath.Point3d;

import org.junit.Test;

/**
 * Class to test the code to generate moments.
 * @author Anthony Bradley
 *
 */
public class TestGenerateMoments {
	
	@Test
	public void testOverall() {
		
		Point3d[] inputArray = new Point3d[8];
		
		inputArray[0] = new Point3d(new double[]{2.0,-16.0,3.0});
		inputArray[1] = new Point3d(new double[]{29.0,10.0,49.0});
		inputArray[2] = new Point3d(new double[]{1.0,65.0,-16.0});
		inputArray[3] = new Point3d(new double[]{13.0,-15.0,-20.0});
		inputArray[4] = new Point3d(new double[]{6.0,22.0,8.0});
		inputArray[5] = new Point3d(new double[]{0.0,12.0,4.0});
		inputArray[6] = new Point3d(new double[]{-10.0,7.0,249.0});
		inputArray[7] = new Point3d(new double[]{13.0,3.0,3.0});

		float[] moments = GenerateMoments.getMoments(inputArray);
		for(float moment : moments){
			System.out.println(moment);
		}
	}

}
