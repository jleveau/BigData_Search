package kmeans1D;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class Kmeans1DEntry implements Writable{
		
	DoubleWritable value;
	Text line;
	int id;
	
	public Kmeans1DEntry(String line, int col) {
		this.line = new Text(line);
		this.value = new DoubleWritable(Double.parseDouble(line.split(",")[col]));
	}
	
	public Kmeans1DEntry(String line, int col,int id) {
		this.line = new Text(line);
		this.value = new DoubleWritable(Double.parseDouble(line.split(",")[col]));
		this.id = id;
	}
	

	public Kmeans1DEntry(Text line, DoubleWritable value){
		this.line = line;
		this.value = value;
	}
	
	public DoubleWritable getValue() {
		return value;
	}

	public void setValue(DoubleWritable value) {
		this.value = value;
	}

	public Text getLine() {
		return line;
	}

	public void setLine(Text line) {
		this.line = line;
	}

	public void readFields(DataInput arg0) throws IOException {
		value.readFields(arg0);
		line.readFields(arg0);
	}
	
	public int getId() {
		return id;
	}
	
	public void setId(int id) {
		this.id = id;
	}

	public void write(DataOutput arg0) throws IOException {
		value.write(arg0);
		line.write(arg0);
	}

	public double distance(double o) {
		return Math.abs(value.get() - o);
	}

}