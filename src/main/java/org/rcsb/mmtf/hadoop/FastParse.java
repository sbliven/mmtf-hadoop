package org.rcsb.mmtf.hadoop;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.dataholders.MmtfStructure;
import org.rcsb.mmtf.decoder.DefaultDecoder;
import org.rcsb.mmtf.decoder.ReaderUtils;
import org.rcsb.mmtf.serialization.MessagePackSerialization;

import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.clustering.*;
import scala.Tuple2;

/**
 * Quickly parse a whole Hadoop sequence file and perform
 * som processing using the {@link StructureDataInterface}.
 * @author Anthony Bradley
 *
 */
public class FastParse {


	/**
	 * A function to read a hadoop sequence file to {@link StructureDataInterface}
	 * and then calculate the fragments found in the PDB.
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args ) throws IOException
	{


		Map<Float,Integer> outMap = new HashMap<>();

		// The input path for the data.
		String inPath = "/Users/anthony/full";
		long startTime = System.currentTimeMillis();
		// This is the default 2 line structure for Spark applications
		SparkConf conf = new SparkConf().setMaster("local[*]")
				.setAppName(FastParse.class.getSimpleName())
				.set("spark.memory.fraction", "0.9")
				.set("spark.memory.storageFraction", "0.1");
		// Set the config for the spark context
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<Vector> fragmentRdd = sc
				.sequenceFile(inPath, Text.class, BytesWritable.class, 8)
				// Roughly thirty seconds
				.mapToPair(t -> new Tuple2<String, byte[]>(t._1.toString(), ReaderUtils.deflateGzip(t._2.getBytes())))
				// Roughly a minute 
				.mapToPair(t -> new Tuple2<String, MmtfStructure>(t._1, new MessagePackSerialization().deserialize(new ByteArrayInputStream(t._2))))
				// Roughly a minute
				.mapToPair(t -> new Tuple2<String, StructureDataInterface>(t._1,  new DefaultDecoder(t._2)))
				// Now find all the fragments in each structure
				.flatMapToPair(new FragmentProteins(8))
				// Now generate the moments for these fragments
				.mapToPair(t -> new Tuple2<String,double[]>(t._1, GenerateMoments.getMoments(t._2)))
				// Convert to vectors for clustering
				.map(t -> Vectors.dense(t._2))
				// Cache them
				.cache();


		// Cluster the data into two classes using KMeans
		int numClusters = 250;
		int numIterations = 10;
		int numRuns = 1;
		// http://theory.stanford.edu/~sergei/papers/vldb12-kmpar.pdf
		String initializationMode = "k-means||";
		// Now train the model
		KMeansModel clusters = KMeans.train(fragmentRdd.rdd(), numClusters, numIterations, numRuns, initializationMode);
		// Compute the cost of this model
	    double WSSSE = clusters.computeCost(fragmentRdd.rdd());
	    System.out.println("Within Set Sum of Squared Errors = " + WSSSE);
	    // Now find the cluster centers
	    for(Vector clusterCenter : clusters.clusterCenters()) {
	    	// Print out this data
	    	System.out.println(clusterCenter.toJson());
	    	System.out.println(clusterCenter.numActives());
	    }
	    // Save the clusters
	    clusters.save(sc.sc(), "model");
		// Now print the number of fragments found
		System.out.println(fragmentRdd.count()+" fragments found.");
		long endTime = System.currentTimeMillis();
		System.out.println("Proccess took "+(endTime-startTime)+" ms.");
		System.out.println(outMap);
		// Now close spark down
		sc.close();
	}
}
