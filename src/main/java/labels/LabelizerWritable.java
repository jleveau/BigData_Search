package labels;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class LabelizerWritable implements Writable {
	DoubleWritable measure;
	Text label;
	
	public LabelizerWritable(){
		this.measure = new DoubleWritable();
		this.label = new Text();
	}

	public LabelizerWritable(String label, double measure) {
		this.measure = new DoubleWritable(measure);
		this.label = new Text(label);
	}

	public void readFields(DataInput arg0) throws IOException {
		label.readFields(arg0);
		measure.readFields(arg0);
	}

	public void write(DataOutput arg0) throws IOException {
		label.write(arg0);
		measure.write(arg0);
	}

	public DoubleWritable getMeasure() {
		return measure;
	}

	public void setMeasure(DoubleWritable measure) {
		this.measure = measure;
	}

	public Text getLabel() {
		return label;
	}

	public void setLabel(Text label) {
		this.label = label;
	}

	@Override
	public String toString() {
		return this.label.toString() + " " + this.measure.toString();
	}
	
}
