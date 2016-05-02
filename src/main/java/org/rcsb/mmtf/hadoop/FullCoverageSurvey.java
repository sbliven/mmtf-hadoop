package org.rcsb.mmtf.hadoop;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.regex.Pattern;

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
 * Analyze biological assemblies in the PDB for whether they fully cover
 * the unit cell
 * @author Spencer Bliven
 *
 */
public class FullCoverageSurvey  implements Serializable {    

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
		String inPath = "/Users/blivens/pdb/full";
		long startTime = System.currentTimeMillis();
		// This is the default 2 line structure for Spark applications
		SparkConf conf = new SparkConf().setMaster("local[*]")
				.setAppName(FullCoverageSurvey.class.getSimpleName());
		// Set the config for the spark context
		JavaSparkContext sc = new JavaSparkContext(conf);
		Pattern xray = Pattern.compile(".*x-?ray.*",Pattern.CASE_INSENSITIVE);
		JavaRDD<String> jprdd = sc
				.sequenceFile(inPath, Text.class, BytesWritable.class, 8)
				//.filter(t -> t._1.toString().equalsIgnoreCase("2VLO"))
				// Roughly thirty seconds
				.mapToPair(t -> new Tuple2<String, byte[]>(t._1.toString(), ReaderUtils.deflateGzip(t._2.getBytes())))
				// Roughly a minute 
				.mapToPair(t -> new Tuple2<String, MmtfStructure>(t._1, new MessagePackSerialization().deserialize(new ByteArrayInputStream(t._2))))
				// Roughly a minute
				.mapToPair(t -> new Tuple2<String, StructureDataInterface>(t._1,  new DefaultDecoder(t._2)))
				// Only X-Ray
				.filter(t -> Arrays.asList(t._2.getExperimentalMethods()).stream().anyMatch(m -> xray.matcher(m).matches() ) )
				.mapToPair(new FullCoverageMapper())
				// Roughly ten minutes to then parse in biojava
				//.mapToPair(new StructDataInterfaceToStructureMapper())
				// Example function counting atoms in those and returning the answer
				//.mapToPair(new ExampleMapper())
				// Now map them into one list
				.map(t -> t._1 + "\t" + t._2);

		String filename = FullCoverageSurvey.class.getSimpleName()+"_"+Math.round(Math.random()*1000);
		System.out.println("Saving to "+filename);
		jprdd.saveAsTextFile(filename);
		// Now print the number of sturctures parsed
		//String results = jprdd.collect().stream().collect(Collectors.joining("\n"));
		long endTime = System.currentTimeMillis();
		System.err.flush();
//		System.out.flush();
//		System.out.println();
//		System.out.println("Results:");
//		System.out.println(results);
		System.err.println("Process took "+(endTime-startTime)+" ms.");
		// Now close spark down
		sc.close();
	}
}