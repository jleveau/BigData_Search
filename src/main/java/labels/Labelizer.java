package labels;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import kmeansnD.KmeansnD;
import kmeansnD.KmeansnDCombinedWritable;
import kmeansnD.KmeansnDCombiner;
import kmeansnD.KmeansnDMapper;
import kmeansnD.kmeansnDReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Labelizer extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		Path input_path;
		Path output_path;
		
		int measure_column;
		int label_column;
		int width;
		try {
			input_path = new Path(args[0]);
			output_path = new Path(args[1]);
			
			label_column = Integer.parseInt(args[2]);
			measure_column = Integer.parseInt(args[3]);
			
			conf.setInt("labels.label_column", label_column);
			conf.setInt("labels.measure_column", measure_column);
			
			for (width = 0; width < args.length - 4; ++width) {
				conf.setInt("labels.class_column." + width,
						Integer.parseInt(args[width + 4]));
			}
			conf.setInt("labels.nb_class_column", width);
		} catch (Exception e) {
			System.err
					.println(" bad arguments, [inputURI][outputURI][label_col][measure_col][class columns ...]");
			return -1;
		}

		FileSystem fs = FileSystem.get(getConf());

		// working directory
		fs.delete(output_path, true);
		fs.mkdirs(output_path);

		while (width > 0){
			labelize(input_path, output_path,width);
		}
		
		return 0;
	}
	
	

	private void labelize(Path input_path, Path output_path, int width) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "labelize");

		job.setJarByClass(Labelizer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(LabelizerWritable.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(LabelizerMapper.class);
		job.setCombinerClass(LabelizerCombiner.class);
		job.setReducerClass(LabelizerReducer.class);

		job.setNumReduceTasks(1);
		
		FileInputFormat.addInputPath(job, input_path);
		FileOutputFormat.setOutputPath(job, new Path(output_path, "out" + width));
				
		job.waitForCompletion(true);	
	}



	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(new Labelizer(), args));
	}
}
