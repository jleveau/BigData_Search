package kmeans1D;

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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Kmeans1DAddColumnMapper extends
		Mapper<LongWritable, Text, NullWritable, Text> {

	ArrayList<Kmeans1DEntry> pivots;
	int k;
	int col;
	Kmeans1DEntry pivot_writable;
	Text text_writable;
	NullWritable null_writable;
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		try {
			double val = Double.parseDouble(value.toString().split(",")[col]);
			String new_value;

			double min_dist = pivots.get(0).distance(val);
			int min_index = 0;
			Iterator<Kmeans1DEntry> it = pivots.iterator();
			it.next();
			int i = 1;

			while (it.hasNext()){
				Kmeans1DEntry pivot = (Kmeans1DEntry) it.next();
				double dist = pivot.distance(val);
				if (dist < min_dist) {
					min_dist = dist;
					min_index = i;
				}
				i++;
			}
			new_value = value.toString() + "," + min_index;
			text_writable.set(new_value);
			context.write(null_writable, text_writable);
		} catch (NumberFormatException e) {
			context.write(null_writable, value);
		}
	}

	@Override
	protected void setup(
			Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		pivots = new ArrayList<Kmeans1DEntry>();
		text_writable = new Text();
		null_writable = NullWritable.get();

		//retreive configuration
		k = conf.getInt("pivots.number", 0);;
		col = conf.getInt("pivots.column_number", 0);
		
		//Test if pivot file exists
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
			line = line.split("\t",-1)[1];
			double val = Double.parseDouble(line);
			pivots.add(new Kmeans1DEntry(new Text(line),
					new DoubleWritable(val)));
		}
	}

}
