package kmeansnD;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KmeansnDMapper extends
Mapper<LongWritable, Text, IntWritable, KmeansnDCombinedWritable>{

	@Override
	protected void map(
			LongWritable key,
			Text value,
			Mapper<LongWritable, Text, IntWritable, KmeansnDCombinedWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.map(key, value, context);
	}

	@Override
	protected void setup(
			Mapper<LongWritable, Text, IntWritable, KmeansnDCombinedWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	}

}
