package labels;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LabelizerReducer extends
		Reducer<Text, LabelizerWritable, Text, LabelizerWritable> {

	HashMap<String, LabelizerWritable> labels_map;
	Text text_w;

	@Override
	protected void setup(
			Reducer<Text, LabelizerWritable, Text, LabelizerWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		labels_map = new HashMap<String, LabelizerWritable>();
		text_w = new Text();
	}

	@Override
	protected void cleanup(
			Reducer<Text, LabelizerWritable, Text, LabelizerWritable>.Context context)
			throws IOException, InterruptedException {
		
		for (Entry<String, LabelizerWritable> entry : labels_map.entrySet()) {
			text_w.set(entry.getKey());

			context.write(text_w, entry.getValue());
		}
	}

	@Override
	protected void reduce(
			Text key,
			Iterable<LabelizerWritable> values,
			Reducer<Text, LabelizerWritable, Text, LabelizerWritable>.Context context)
			throws IOException, InterruptedException {
		// Adding coordinates from reducers
		Iterator<LabelizerWritable> it = values.iterator();

		while (it.hasNext()) {
			LabelizerWritable val = it.next();

			if (labels_map.containsKey(key.toString())) {
				if (labels_map.get(key.toString()).getMeasure().get() < val
						.getMeasure().get()) {
					LabelizerWritable label_w = labels_map.get(key.toString());
					label_w.setLabel(new Text(val.getLabel().toString()));
					label_w.setMeasure(new DoubleWritable(val.getMeasure().get()));
				}
			} else {
				labels_map.put(key.toString(), new LabelizerWritable(val
						.getLabel().toString(), val.getMeasure().get()));
			}
		}
	}
}