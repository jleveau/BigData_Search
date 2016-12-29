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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Kmeans1DMapper extends
		Mapper<LongWritable, Text, IntWritable, Kmeans1DCombinedWritable> {

	ArrayList<Kmeans1DEntry> pivots;
	int k;
	int col;
	Kmeans1DEntry pivot_writable;
	DoubleWritable value_writable;
	Text line_writable;

	@Override
	protected void map(
			LongWritable key,
			Text value,
			Mapper<LongWritable, Text, IntWritable, Kmeans1DCombinedWritable>.Context context)
			throws IOException, InterruptedException {
		try {
			double val = Double.parseDouble(value.toString().split(",")[col]);

			Kmeans1DCombinedWritable entry = new Kmeans1DCombinedWritable(1,
					val);

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
			
			context.write(new IntWritable(min_index), entry);
		} catch (NumberFormatException e) {

		}
	}

	@Override
	protected void setup(
			Mapper<LongWritable, Text, IntWritable, Kmeans1DCombinedWritable>.Context context)
			throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		pivots = new ArrayList<Kmeans1DEntry>();
		value_writable = new DoubleWritable();
		line_writable = new Text();

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

		/*
		 * SequenceFile.Reader reader = new SequenceFile.Reader(conf,
		 * Reader.file(new Path("/resources/pivots")));
		 * 
		 * Text key = new Text(); IntWritable val = new IntWritable();
		 * 
		 * while (reader.next(key, val)) { System.err.println(key + "\t" + val);
		 * }
		 * 
		 * reader.close();
		 */
	}

}
