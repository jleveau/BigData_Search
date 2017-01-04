package labels;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LabelizerReducer extends Reducer<IntWritable, LabelizerWritable, NullWritable, Text> {

}
