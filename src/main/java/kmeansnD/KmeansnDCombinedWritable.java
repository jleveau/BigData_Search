package kmeansnD;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Writable;

public class KmeansnDCombinedWritable implements Writable{
	
	ArrayList<Double> coordinates;
	int num;
	int dimension;
	
	KmeansnDCombinedWritable(){
		coordinates = new ArrayList<Double>();
		num = 0;
		dimension = 0;
	}
	
	public KmeansnDCombinedWritable(int dim) {
		coordinates = new ArrayList<Double>();
		num = 0;
		this.dimension = dim;
		for (int i=0; i < dim; i++){
			coordinates.add(0.0);
		}
	}
	
	void add(KmeansnDCombinedWritable o){

		ArrayList<Double> this_coordinates = this.coordinates;
		ArrayList<Double> other_coordinates = o.getCoordinates();
		for (int i=0; i<this.coordinates.size(); ++i){
			this_coordinates.set(i, this_coordinates.get(i) + other_coordinates.get(i));
		}
		this.num += o.getNum();
	}
	
	void average(){
		for (int i=0; i<coordinates.size(); ++i){
			this.coordinates.set(i, this.coordinates.get(i) / this.num);
		}
	}
	
	public ArrayList<Double> getCoordinates() {
		return coordinates;
	}

	public void setCoordinates(ArrayList<Double> coordinates) {
		this.coordinates = coordinates;
		this.dimension = coordinates.size();
	}

	public int getNum() {
		return num;
	}

	public void setNum(int num) {
		this.num = num;
	}

	KmeansnDCombinedWritable(ArrayList<Double> coordinates, int num){
		this.coordinates = coordinates;
		this.num = num;
		this.dimension = coordinates.size();
	}

	public void readFields(DataInput arg0) throws IOException {
		coordinates = new ArrayList<Double>();
		this.dimension = arg0.readInt();
		for (int i=0; i < this.dimension; ++i){
			this.coordinates.add(arg0.readDouble());
		}
		num = arg0.readInt();
	}

	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(this.dimension);
		for (int i=0; i< this.dimension;i++){
			arg0.writeDouble(this.coordinates.get(i));
		}
		arg0.writeInt(this.num);
		
	}

	public int getDimension() {
		return dimension;
	}

	public void setDimension(int dimension) {
		this.dimension = dimension;
	}

}
