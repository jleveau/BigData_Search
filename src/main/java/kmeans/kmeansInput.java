package kmeans;

import java.awt.geom.Point2D;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface kmeansInput {
	
	public void readFields(DataInput arg0) throws IOException;
	
	public void write(DataOutput arg0) throws IOException;

	public int distance(Point2D o);
	
}
