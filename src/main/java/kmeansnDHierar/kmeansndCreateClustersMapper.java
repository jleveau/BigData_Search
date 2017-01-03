package kmeansnDHierar;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;

import kmeansnD.KmeansnDCombinedWritable;
import kmeansnD.Pivot;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class kmeansndCreateClustersMapper extends
		Mapper<NullWritable, KmeansnDCombinedWritable, NullWritable, KmeansnDCombinedWritable> {

	KmeansnDCombinedWritable combined_writable;
	int nb_dimensions;
	ArrayList<Integer> columns;
	NullWritable null_writable;
	ArrayList<Pivot> pivots;
	int k, dim;
	MultipleOutputs<NullWritable, KmeansnDCombinedWritable> outputs;
	
	@Override
	protected void map(
			NullWritable key,
			KmeansnDCombinedWritable value,
			Mapper<NullWritable, KmeansnDCombinedWritable, NullWritable, KmeansnDCombinedWritable>.Context context)
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
		value.getIndexes().add(min_index);

		outputs.write(null_writable, value, Integer.toString(min_index));
	}

	@Override
	protected void cleanup(
			Mapper<NullWritable, KmeansnDCombinedWritable, NullWritable, KmeansnDCombinedWritable>.Context context)
			throws IOException, InterruptedException {
		outputs.close();
	}

	@Override
	protected void setup(
			Mapper<NullWritable, KmeansnDCombinedWritable, NullWritable, KmeansnDCombinedWritable>.Context context)
			throws IOException, InterruptedException {

		null_writable = NullWritable.get();
		
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		pivots = new ArrayList<Pivot>();

		k = conf.getInt("pivots.number", 0);
		dim = conf.getInt("pivots.dimension", 0);

		outputs = new MultipleOutputs<NullWritable, KmeansnDCombinedWritable>(context);
		
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
