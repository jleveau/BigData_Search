package kmeans1D;



import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class Kmeans1DCombinedWritable implements Writable{

	int num;
	double coordinates;
	
	public Kmeans1DCombinedWritable(){
		
	}
	
	public Kmeans1DCombinedWritable(int num, double coordinates) {
		super();
		this.num = num;
		this.coordinates = coordinates;
	}

	public void readFields(DataInput arg0) throws IOException {
		num = arg0.readInt();
		coordinates = arg0.readDouble();
	}

	public int getNum() {
		return num;
	}
	public void setNum(int num) {
		this.num = num;
	}
	public double getCoordinates() {
		return coordinates;
	}
	public void setCoordinates(double coordinates) {
		this.coordinates = coordinates;
	}
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(num);
		arg0.writeDouble(coordinates);
	}

	public void add(Kmeans1DCombinedWritable val) {
		coordinates += val.coordinates;
		num += val.num;
	}
	
}