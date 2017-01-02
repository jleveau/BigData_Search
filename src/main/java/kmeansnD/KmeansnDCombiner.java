package kmeansnD;


import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;



public class KmeansnDCombiner extends Reducer<IntWritable,KmeansnDCombinedWritable,IntWritable, 
												KmeansnDCombinedWritable>  {

	KmeansnDCombinedWritable combined_writable;
	int dim;

	@Override
	protected void reduce(IntWritable key, Iterable<KmeansnDCombinedWritable> values,
			Reducer<IntWritable, KmeansnDCombinedWritable, IntWritable, KmeansnDCombinedWritable>.Context context)
			throws IOException, InterruptedException {
				combined_writable = new KmeansnDCombinedWritable(dim);

		for (KmeansnDCombinedWritable val : values){
			combined_writable.add(val);
		}
		context.write(key, combined_writable);
	}

	@Override
	protected void setup(
			Reducer<IntWritable, KmeansnDCombinedWritable, IntWritable, KmeansnDCombinedWritable>.Context context)
			throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		dim = conf.getInt("pivots.dimension", 0);

	}
	
}