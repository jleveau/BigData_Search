package kmeansnD;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperCopyToSequenceFile extends
		Mapper<LongWritable, Text, NullWritable, KmeansnDCombinedWritable> {

	KmeansnDCombinedWritable combined_writable;
	int nb_dimensions;
	ArrayList<Integer> columns;
	NullWritable null_writable;

	@Override
	protected void map(
			LongWritable key,
			Text value,
			Mapper<LongWritable, Text, NullWritable, KmeansnDCombinedWritable>.Context context)
			throws IOException, InterruptedException {

		// Retrieve columns from the input
		String[] splits = value.toString().split(",");

		// Read double in the line, at index defined in columns
		ArrayList<Double> coordinates = new ArrayList<Double>();
		ArrayList<Integer> indexes = new ArrayList<Integer>();
		try {
			for (int i = 0; i < nb_dimensions; ++i) {
				coordinates.add(Double.parseDouble(splits[columns.get(i)]));
			}
		} catch (NumberFormatException e) {
			return;
		}
		
		//Read int to retreive previous index 
		for (int i=nb_dimensions; i<splits.length; ++i){
			indexes.add(Integer.parseInt(splits[i]));
		}

		combined_writable.setIndexes(indexes);
		combined_writable.setCoordinates(coordinates);
		combined_writable.setNum(1);

		context.write(null_writable, combined_writable);
	}

	@Override
	protected void setup(
			Mapper<LongWritable, Text, NullWritable, KmeansnDCombinedWritable>.Context context)
			throws IOException, InterruptedException {

		combined_writable = new KmeansnDCombinedWritable();
		columns = new ArrayList<Integer>();
		null_writable = NullWritable.get();

		Configuration conf = context.getConfiguration();
		nb_dimensions = conf.getInt("pivots.dimension", 0);
		// Define the index to read for coordinates
		for (int i = 0; i < nb_dimensions; i++) {
			columns.add(new Integer(conf.getInt("pivots.column_number." + i, 0)));
		}
	}

}
