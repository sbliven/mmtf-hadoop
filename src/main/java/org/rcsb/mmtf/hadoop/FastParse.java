package org.rcsb.mmtf.hadoop;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.vecmath.Point3d;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.dataholders.MmtfStructure;
import org.rcsb.mmtf.decoder.DefaultDecoder;
import org.rcsb.mmtf.decoder.ReaderUtils;
import org.rcsb.mmtf.serialization.MessagePackSerialization;

import scala.Tuple2;

/**
 * Quickly parse a whole Hadoop sequence file and perform
 * som processing using the {@link StructureDataInterface}.
 * @author Anthony Bradley
 *
 */
public class FastParse {


	private static final int MAX_CLUSTERS = 10000;
	/**
	 * A function to read a hadoop sequence file to {@link StructureDataInterface}
	 * and then calculate the fragments found in the PDB.
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args ) throws IOException
	{

		
		float[] rmsdAttempts = new float[] {1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f, 9.0f, 10.0f};
		Map<Float,Integer> outMap = new HashMap<>();

		// The input path for the data.
		String inPath = "/Users/anthony/full";
		long startTime = System.currentTimeMillis();
		// This is the default 2 line structure for Spark applications
		SparkConf conf = new SparkConf().setMaster("local[*]")
				.setAppName(FastParse.class.getSimpleName());
		conf.set("spark.memory.offHeap.enabled", "true");
		conf.set("spark.memory.fraction", "0.9");
		conf.set("spark.memory.offHeap.size", "20g");
		// Set the config for the spark context
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaPairRDD<String, List<Point3d>> fragmentRdd = sc
				.sequenceFile(inPath, Text.class, BytesWritable.class, 8)
				// Roughly thirty seconds
				.mapToPair(t -> new Tuple2<String, byte[]>(t._1.toString(), ReaderUtils.deflateGzip(t._2.getBytes())))
				// Roughly a minute 
				.mapToPair(t -> new Tuple2<String, MmtfStructure>(t._1, new MessagePackSerialization().deserialize(new ByteArrayInputStream(t._2))))
				// Roughly a minute
				.mapToPair(t -> new Tuple2<String, StructureDataInterface>(t._1,  new DefaultDecoder(t._2)))
				// Now find all the fragments in this chain
				.flatMapToPair(new FragmentProteins(8));

		for(float rmsd : rmsdAttempts){
			// Cache the data
			JavaPairRDD<String, List<Point3d>> cachedFragmentRdd = fragmentRdd.cache();
			int numClusters = 0;
			for(int i=0; i<MAX_CLUSTERS; i++){
				// Take this as your first sample
				Tuple2<String, List<Point3d>> sampleFragment = cachedFragmentRdd.takeSample(false, 1).get(0);
				cachedFragmentRdd = cachedFragmentRdd.filter(new RMSDFilter(sampleFragment._2(),rmsd));
				numClusters++;
				if(cachedFragmentRdd.count()<10000){
					outMap.put(rmsd, numClusters);
					break;
				}
				System.out.println("RDD still has: "+cachedFragmentRdd.count()+" fragments");
			}
		}
		// Now print the number of fragments found
		System.out.println();
		System.out.println(fragmentRdd.count()+" fragments found.");
		long endTime = System.currentTimeMillis();
		System.out.println("Proccess took "+(endTime-startTime)+" ms.");
		System.out.println(outMap);
		// Now close spark down
		sc.close();
	}
}
