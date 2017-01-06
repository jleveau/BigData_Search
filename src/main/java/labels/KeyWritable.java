package labels;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class KeyWritable implements Writable, WritableComparable<KeyWritable> {

	Text data;
	
	public KeyWritable(){
		
	}

	public KeyWritable(String coordinates) {
		this.data = new Text(coordinates);
	}

	public void readFields(DataInput arg0) throws IOException {
		data = new Text();
		data.readFields(arg0);
	}

	public void write(DataOutput arg0) throws IOException {
		data.write(arg0);
	}

	@Override
	public String toString() {
		return data.toString();
	}

	public int compareTo(KeyWritable o) {
		return data.toString().compareTo(o.toString());
	}

	public void set(Text text) {
		this.data = text;	
	}

}
