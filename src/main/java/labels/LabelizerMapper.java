package labels;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LabelizerMapper extends
		Mapper<LongWritable, Text, Text, LabelizerWritable> {

	private static HashMap<String, LabelizerWritable> labels_map;
	private int nb_class_col;
	private int measure_col;
	private int label_col;
	private int[] class_col;
	private StringBuilder builder;
	private Text text_w;

	@Override
	protected void cleanup(
			Mapper<LongWritable, Text, Text, LabelizerWritable>.Context context)
			throws IOException, InterruptedException {

		Iterator<Entry<String, LabelizerWritable>> map_it = labels_map
				.entrySet().iterator();

		while (map_it.hasNext()) {
			Entry<String, LabelizerWritable> entry = map_it.next();
			text_w.set(entry.getKey());
			context.write(text_w, entry.getValue());
		}
	}

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, LabelizerWritable>.Context context)
			throws IOException, InterruptedException {
		String label;
		String coordinates;
		double measure;
		builder.setLength(0);

		String[] splits = value.toString().split(",");

		// Check if measure column exists
		try {
			measure = Double.parseDouble(splits[measure_col]);
		} catch (NumberFormatException e) {
			return;
		}

		for (int i = 0; i < nb_class_col; ++i) {
			builder.append(splits[class_col[i]]);
			builder.append(" ");
		}
		builder.setLength(builder.length());

		coordinates = builder.toString();

		label = splits[label_col];

		if (labels_map.containsKey(coordinates)) {
			LabelizerWritable writable = labels_map.get(coordinates);

			if (writable.getMeasure().get() < measure) {
				writable.getLabel().set(label);
				writable.getMeasure().set(measure);
			}
		} else {
			labels_map.put(coordinates, new LabelizerWritable(label, measure));

		}
	}

	@Override
	protected void setup(
			Mapper<LongWritable, Text, Text, LabelizerWritable>.Context context)
			throws IOException, InterruptedException {

		builder = new StringBuilder();
		text_w = new Text();

		// Init data structures
		labels_map = new HashMap<String, LabelizerWritable>();

		// Retreive data from configuration
		Configuration conf = context.getConfiguration();
		nb_class_col = conf.getInt("labels.nb_class_column", 0);
		measure_col = conf.getInt("labels.measure_column", 0);
		label_col = conf.getInt("labels.label_column", 0);
		class_col = new int[nb_class_col];
		for (int i = 0; i < nb_class_col; i++) {
			class_col[i] = conf.getInt("labels.class_column." + i, 0);
		}
	}
}