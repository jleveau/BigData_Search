package kmeans1D;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Kmeans1DCombiner extends Reducer<IntWritable,Kmeans1DCombinedWritable,IntWritable, Kmeans1DCombinedWritable> 
{
	Kmeans1DCombinedWritable combined_writable;

	@Override
	protected void reduce(IntWritable key, Iterable<Kmeans1DCombinedWritable> values,
			Reducer<IntWritable, Kmeans1DCombinedWritable, IntWritable, Kmeans1DCombinedWritable>.Context context)
			throws IOException, InterruptedException {
		IntWritable pivot = key;
		combined_writable = new Kmeans1DCombinedWritable(0,0.0);

		for (Kmeans1DCombinedWritable val : values){
			combined_writable.add(val);
		}
		context.write(pivot, combined_writable);
	}
	
}