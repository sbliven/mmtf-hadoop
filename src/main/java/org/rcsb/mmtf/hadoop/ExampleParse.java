package org.rcsb.mmtf.hadoop;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;

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

import scala.Tuple2;


/**
 * Example parsers using thre Hadoop  sequence file
 * @author Anthony Bradley
 *
 */
public class ExampleParse  implements Serializable {    

	/**
	 * Serial ID for this version of the class.
	 */
	private static final long serialVersionUID = 3037567648753603114L;
	
	/**
	 * A function to read a hadoop sequence file to Biojava structures.
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args ) throws IOException
	{
		// The input path for the data.
		String inPath = "/Users/anthony/full";
		long startTime = System.currentTimeMillis();
		// This is the default 2 line structure for Spark applications
		SparkConf conf = new SparkConf().setMaster("local[*]")
				.setAppName(ExampleParse.class.getSimpleName());
		// Set the config for the spark context
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<Double> atomConactRdd = sc
				.sequenceFile(inPath, Text.class, BytesWritable.class, 8)
				// Use this for testing (set's the fraction of the data to process) 1.0 means all
//				.sample(false, 1.0)
				// Roughly thirty seconds
				.mapToPair(t -> new Tuple2<String, byte[]>(t._1.toString(), ReaderUtils.deflateGzip(t._2.getBytes())))
				// Roughly a minute 
				.mapToPair(t -> new Tuple2<String, MmtfStructure>(t._1, new MessagePackSerialization().deserialize(new ByteArrayInputStream(t._2))))
				// Roughly a minute
				.mapToPair(t -> new Tuple2<String, StructureDataInterface>(t._1,  new DefaultDecoder(t._2)))
				// Roughly ten minutes to then parse in biojava
//				.mapToPair(new StructDataInterfaceToStructureMapper())
				// Example function counting atoms in those and returning the answer
//				.mapToPair(new ExampleMapper())
				// Example function iterating through and finding the distances
				.flatMap(new CalculateContacts(5.0))
				// Now cache the rdd (so you don't recalculate on each action on the rdd
				.map(t -> t.getDistance());
		// Now print the number of contacts found
		System.out.println(atomConactRdd.count()+" contacts.");
		long endTime = System.currentTimeMillis();
		System.out.println("Proccess took "+(endTime-startTime)+" ms.");
		// Now close spark down
		sc.close();
	}
}
