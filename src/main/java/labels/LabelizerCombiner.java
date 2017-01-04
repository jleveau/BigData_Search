package labels;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class LabelizerCombiner extends
		Reducer<IntWritable, LabelizerWritable, IntWritable, LabelizerWritable> {

}
