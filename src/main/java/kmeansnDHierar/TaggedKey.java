package kmeansnDHierar;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class TaggedKey implements WritableComparable<TaggedKey>, Writable {
	
	private Text joinKey;
	private IntWritable tag;

	
	TaggedKey(){
		joinKey = new Text();
		tag = new IntWritable();
	}

	public void readFields(DataInput arg0) throws IOException {
		joinKey.readFields(arg0);
		tag.readFields(arg0);
	}

	public void write(DataOutput arg0) throws IOException {
		joinKey.write(arg0);
		tag.write(arg0);
	}

	public int compareTo(TaggedKey other) {
		int comparedValue = this.joinKey.compareTo(other.getJoinKey());
		if (comparedValue == 0){
			this.tag.compareTo(other.getTag());
		}
		return comparedValue;
	}

	public Text getJoinKey() {
		return joinKey;
	}

	public void setJoinKey(String s) {
		this.joinKey = new Text(s);
	}

	public IntWritable getTag() {
		return tag;
	}

	public void setTag(int tag) {
		this.tag = new IntWritable(tag);
	}

	public void set(String s, int order){
		this.joinKey.set(s);
		this.tag.set(order);
	}
}
