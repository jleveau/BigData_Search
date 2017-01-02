package kmeans1D;


import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class kmeans1DReducer extends Reducer<IntWritable,Kmeans1DCombinedWritable,IntWritable, DoubleWritable> 
{
	Kmeans1DCombinedWritable combined_writable;
	DoubleWritable double_writable;

	@Override
	protected void setup(
			Reducer<IntWritable, Kmeans1DCombinedWritable, IntWritable, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		double_writable = new DoubleWritable();
	}

	@Override
	protected void reduce(IntWritable key, Iterable<Kmeans1DCombinedWritable> values,
			Reducer<IntWritable, Kmeans1DCombinedWritable, IntWritable, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		int num = 0;
		double coordinates = 0.0;
		for (Kmeans1DCombinedWritable val : values){
			num += val.getNum();
			coordinates += val.getCoordinates();
		}
		coordinates /= num;
		context.getCounter("SUM", "after").increment((long) coordinates);

		double_writable.set(coordinates);
		context.write(key, double_writable);
	}
}