package kmeansnD;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;

import kmeans1D.Kmeans1DEntry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class kmeansnDAddColumnMapper extends
		Mapper<LongWritable, Text, NullWritable, Text> {

	ArrayList<Pivot> pivots;
	int k;
	int dim;
	Kmeans1DEntry pivot_writable;
	Text text_writable;
	NullWritable null_writable;
	ArrayList<Integer> columns;


	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		Iterator<Pivot> iterator;
		iterator = pivots.iterator();

		
		String[] splits = value.toString().split(",");

		// Read double in the line, at index defined in columns
		ArrayList<Double> coordinates = new ArrayList<Double>();
		try {
			for (int i = 0; i < dim; ++i) {
				coordinates.add(Double.parseDouble(splits[columns.get(i)]));
			}
		} catch (NumberFormatException e) {
			return;
		}

		//Searching the index of nearest pivot
		Pivot pivot = iterator.next();

		double min_dist = pivot.distance(coordinates);
		int min_index = pivots.get(0).getIndex();
		
		while (iterator.hasNext()) {
			pivot = iterator.next();
			double distance = pivot.distance(coordinates);
			if (distance < min_dist) {
				min_dist = distance;
				min_index = pivot.getIndex();
			}
		}
		
		text_writable.set(value.toString() + "," + min_index);
		context.write(null_writable, text_writable);
	}

	@Override
	protected void setup(
			Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {

		null_writable = NullWritable.get();
		text_writable = new Text();
		
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		pivots = new ArrayList<Pivot>();

		k = conf.getInt("pivots.number", 0);
		dim = conf.getInt("pivots.dimension", 0);
		columns = new ArrayList<Integer>();

		// Define the index to read for coordinates
		for (int i = 0; i < dim; i++) {
			columns.add(new Integer(conf.getInt("pivots.column_number." + i, 0)));
		}

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
			Pivot p = new Pivot();
			String line = reader.readLine();
			try {
				String[] splits_line = line.split("\t", -1);
				p.setIndex(Integer.parseInt(splits_line[0]));

				line = line.split("\t", -1)[1];
				String[] splits_coordinates = line.split(",");
				ArrayList<Double> coordinates = new ArrayList<Double>();
				for (int j = 0; j < dim; ++j) {
					coordinates.add(Double.parseDouble(splits_coordinates[j]));
				}
				p.setCoordinates(coordinates);
				pivots.add(p);
			} catch (NumberFormatException e) {

			}
		}
	}
}
