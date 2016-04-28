package org.rcsb.mmtf.hadoop;

import java.util.List;

import javax.vecmath.Point3d;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

/**
 * Class to filter a list of fragments on RMSD.
 * @author Anthony Bradley
 *
 */
public class RMSDFilter implements Function<Tuple2<String, List<Point3d>>, Boolean> {

	
	/**
	 * 
	 */
	private static final long serialVersionUID = -8406639613937304310L;
	private List<Point3d> filterItem;
	private float rmsd;
	
	/**
	 * Constructor to intiliase shared Filter object.
	 * @param filterItem
	 */
	public RMSDFilter(List<Point3d> filterItem, float rmsd) {
		this.filterItem = filterItem;
		this.rmsd = rmsd;
	}
	
	@Override
	public Boolean call(Tuple2<String, List<Point3d>> v1) throws Exception {
		QCPUpdateable qcpUpdateable = new QCPUpdateable();
		Point3d[] x = this.filterItem.toArray(new Point3d[0]);
		Point3d[] y =  v1._2.toArray(new Point3d[0]);
		qcpUpdateable.set(x, y);
		if(qcpUpdateable.getRmsd()>rmsd){
			return true;
		}
		return false;
	}

}
