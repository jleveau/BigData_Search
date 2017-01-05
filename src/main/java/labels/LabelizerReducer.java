package labels;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LabelizerReducer extends Reducer<Text, LabelizerWritable, Text, LabelizerWritable> {

	LabelizerWritable max;

	@Override
	protected void reduce(Text key, Iterable<LabelizerWritable> values,
			Reducer<Text, LabelizerWritable, Text, LabelizerWritable>.Context context)
			throws IOException, InterruptedException {
		//Adding coordinates from reducers		
		Iterator<LabelizerWritable> it = values.iterator();
		max = it.next();
		while(it.hasNext()){
			LabelizerWritable val = it.next();
			if (val.getMeasure().get() > max.getMeasure().get())
				max = val;
		}
		context.write(key, max);
	}

}
