package kmeansnD;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class KmeansnDMapper
		extends
		Mapper<NullWritable, KmeansnDCombinedWritable, IntWritable, KmeansnDCombinedWritable> {

	int k;
	ArrayList<Pivot> pivots;
	ArrayList<Integer> columns;
	IntWritable pivot_key_writable;
	int dim;


	@Override
	protected void map(
			NullWritable key,
			KmeansnDCombinedWritable value,
			Mapper<NullWritable, KmeansnDCombinedWritable, IntWritable, KmeansnDCombinedWritable>.Context context)
			throws IOException, InterruptedException {

		Iterator<Pivot> iterator;
		iterator = pivots.iterator();

		Pivot pivot = iterator.next();
		double min_dist = pivot.distance(value.getCoordinates());
		int min_index = 0;
		int i = 1;

		while (iterator.hasNext()) {
			pivot = iterator.next();

			double distance = pivot.distance(value.getCoordinates());

			if (distance < min_dist) {
				min_dist = distance;
				min_index = i;
			}
			++i;
		}
		
		pivot_key_writable.set(min_index);
		context.write(pivot_key_writable, value);
	}

	@Override
	protected void setup(
			Mapper<NullWritable, KmeansnDCombinedWritable, IntWritable, KmeansnDCombinedWritable>.Context context)
			throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		pivots = new ArrayList<Pivot>();
		pivot_key_writable = new IntWritable();

		k = conf.getInt("pivots.number", 0);
		dim = conf.getInt("pivots.dimension", 0);

		// Test if pivot file exists
		if (context.getCacheFiles().length == 0)
			throw new IOException("No pivot file");
		URI uri = null;
		try {
			uri = new URI(conf.get("pivots.uri"));
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}

		BufferedReader reader = new BufferedReader(new InputStreamReader(
				fs.open(new Path(uri))));

		// Create pivots
		for (int i = 0; i < k; ++i) {
			String line = reader.readLine();
			try {
				line = line.split("\t", -1)[1];
				String[] splits = line.split(",");
				ArrayList<Double> coordinates = new ArrayList<Double>();
				for (int j = 0; j < dim; ++j) {
					coordinates.add(Double.parseDouble(splits[j]));
				}
				pivots.add(new Pivot(coordinates));
			} catch (NumberFormatException e) {

			}
		}
	}
}