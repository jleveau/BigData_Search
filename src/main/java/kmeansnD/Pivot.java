package kmeansnD;

import java.util.ArrayList;

public class Pivot {

	ArrayList<Double> coordinates;
	int index;
	
	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public Pivot() {
		super();
		this.coordinates = new ArrayList<Double>();
	}

	public Pivot(ArrayList<Double> coordinates) {
		super();
		this.coordinates = coordinates;
	}
	
	public double distance(ArrayList<Double> other){
		
		double diff_sum = 0;
		int dim = coordinates.size();
		
		for (int i =0; i < dim; ++i){
			diff_sum += (coordinates.get(i) - other.get(i)) * (coordinates.get(i) - other.get(i));
		}
	
		return Math.sqrt(diff_sum);	
	}

	public ArrayList<Double> getCoordinates() {
		return coordinates;
	}

	public void setCoordinates(ArrayList<Double> coordinates) {
		this.coordinates = coordinates;
	}
	
}
