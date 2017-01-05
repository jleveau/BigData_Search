package kmeansnD;


import java.io.IOException;
import java.util.ArrayList;

import kmeans1D.Kmeans1DCombinedWritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class kmeansnDReducer extends Reducer<IntWritable,KmeansnDCombinedWritable,IntWritable, Text> {
	
	int dim;
	double[] coordinates;
	Text text_writable;
	
	@Override
	protected void setup(
			Reducer<IntWritable, KmeansnDCombinedWritable, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		dim = conf.getInt("pivots.dimension", 0);

		coordinates = new double[dim];
		text_writable = new Text();
	}

	@Override
	protected void reduce(IntWritable key, Iterable<KmeansnDCombinedWritable> values,
			Reducer<IntWritable, KmeansnDCombinedWritable, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		int num = 0;
		StringBuilder builder = new StringBuilder();
		
		//Initialize coordinates to 0
		for (int i=0; i<dim; ++i){
			coordinates[i]= 0 ;
		}
		
		//Adding coordinates from reducers
		for (KmeansnDCombinedWritable val : values){
			for (int i=0; i<dim; ++i){
				coordinates[i] += val.getCoordinates().get(i);
			}
			num += val.getNum();
		}

		//average and updating counter
		for (int i=0; i<dim; ++i){
			coordinates[i] /= num;
			builder.append(Double.toString(coordinates[i]));
			builder.append(",");
			context.getCounter("SUM", "after").increment((long) coordinates[i]);
		}

		//remove last coma
		builder.setLength(builder.length()-1);
		
		//write
		text_writable.set(builder.toString());

		context.write(key, text_writable);
	}
}