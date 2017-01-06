package labels;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class LabelizerPartitioner extends Partitioner<Text, LabelizerWritable> {

	@Override
	public int getPartition(Text key, LabelizerWritable value, int num) {
		return (key.toString().hashCode() & Integer.MAX_VALUE) % num;
	}
	
}