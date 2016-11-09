package kmeans;

import java.awt.geom.Point2D;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;


public class Points2DMeansWritable implements Writable, kmeansInput{
		
	Point2D point;
	

	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub

	}
	
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub

	}

	public int distance(Point2D o) {
		// TODO Auto-generated method stub
		return 0;
	}

}