package kmeansnDHierar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import kmeans.Kmeans;
import kmeans1D.Kmeans1DCombiner;
import kmeans1D.kmeans1DReducer;
import kmeansnD.KmeansnD;
import kmeansnD.KmeansnDCombinedWritable;
import kmeansnD.KmeansnDCombiner;
import kmeansnD.KmeansnDMapper;
import kmeansnD.kmeansnDAddColumnMapper;
import kmeansnD.kmeansnDReducer;

public class KmeansnDHierar extends Configured implements Tool {

	int k;
	int depth;

	public int run(String[] args) throws Exception, ClassNotFoundException,
			InterruptedException {

		Configuration conf = getConf();
		Path input_path;
		Path output_path;

		try {
			input_path = new Path(args[0]);
			output_path = new Path(args[1]);
			k = Integer.parseInt(args[2]);
			depth = Integer.parseInt(args[3]);
			conf.setInt("pivots.number", k);
			conf.setInt("hierarchie.depth", depth);
			int i;
			for (i = 0; i < args.length - 4; ++i) {
				conf.setInt("pivots.column_number." + i,
						Integer.parseInt(args[i + 4]));
			}
			conf.setInt("pivots.dimension", i);
		} catch (Exception e) {
			System.err
					.println(" bad arguments, [inputURI] [outputURI][nb_pivot][depth][num_columns ...]");
			return -1;
		}

		FileSystem fs = FileSystem.get(getConf());

		// working directory
		fs.delete(output_path, true);
		fs.mkdirs(output_path);

		kmeans(input_path, output_path, conf, depth, 0);

		return 0;
	}

	private void kmeans(Path input_path, Path output_path, Configuration conf,
			int depth_acc, int k_acc) throws IOException, URISyntaxException,
			IllegalArgumentException, ClassNotFoundException,
			InterruptedException {

		FileSystem fs = FileSystem.get(getConf());
		
		if (depth_acc == 0) {
			System.out.println("stop recursion");
			//Path result = new Path(output_path, "result");
			//FileUtil.copyMerge(arg0, arg1, arg2, arg3, arg4, arg5, arg6)
			return;
		}
		System.out.println("start iteration for depth = " + depth_acc + " k = "
				+ k_acc);
		System.out.println("input path = " + input_path.toString());
		System.out.println("output path = " + output_path.toString());

		String pivot_path;
		String base_path = new Path(output_path, "_iterations").toString();

		// create initial pivot file
		pivot_path = new Path(output_path, "starting_pivots").toString();
		createPivots(input_path.toString(), pivot_path, k);

		System.out.println("pivot path = " + pivot_path.toString());

		// Copy input as SequenceFile
		Path new_input_path = new Path(output_path, "input");
		fs.delete(new_input_path, true);
		copyAsSequenceFile(input_path, new_input_path);
		input_path = new Path(new_input_path, "part-m-00000");

		int iteration = 0;

		// Create first job using default pivots
		Job job = updatePivotsJob(pivot_path, iteration);
		FileInputFormat.addInputPath(job, input_path);
		FileOutputFormat.setOutputPath(job,
				new Path(base_path, Integer.toString(iteration)));
		job.getConfiguration().set("pivots.uri", pivot_path.toString());

		Path iteration_output_previous = new Path(base_path,
				Integer.toString(iteration));
		Path iteration_output_current;
		if (!job.waitForCompletion(false))
			return;
		long before = job.getCounters().findCounter("SUM", "after").getValue();
		long after;

		// Start iterations
		boolean hasUpdates = true;
		while (hasUpdates) {
			++iteration;

			// create the new job using the output of previous job
			job = updatePivotsJob(pivot_path, iteration);
			FileInputFormat.addInputPath(job, input_path);

			// iteration-1 to take the previous result
			iteration_output_current = new Path(base_path,
					Integer.toString(iteration));
			FileOutputFormat.setOutputPath(job, iteration_output_current);
			pivot_path = new Path(iteration_output_previous.toString(),
					"part-r-00000").toString();

			// Define the pivot uri for mappers
			job.getConfiguration().set("pivots.uri", pivot_path.toString());

			// Start job
			if (!job.waitForCompletion(false))
				return;

			// compute end condition
			after = job.getCounters().findCounter("SUM", "after").getValue();
			if (Math.abs(after - before) < 1) {
				hasUpdates = false;
			}
			before = after;
			iteration_output_previous = iteration_output_current;
		}
		System.out.println("creating new clusters sequencefile");
		// create new files for next clusters
		Path output_for_cluster = new Path(output_path.toString(), "clusters_"
				+ Integer.toString(k_acc) + "_" + Integer.toString(depth_acc));

		createNewClustersFile(input_path, output_for_cluster, new Path(
				iteration_output_previous.toString(), "part-r-00000"), k);

		// Delete sequenceFile
		fs.delete(input_path.getParent(), true);
		fs.delete(new Path(base_path), true);

		// Start new depth
		for (int i = 0; i < k; ++i) {
			kmeans(new Path(output_for_cluster.toString(), i + "-m-00000"),
					output_path, conf, depth_acc - 1, i);
		}
	}

	private Job updatePivotsJob(String pivot_path, int iteration)
			throws IOException, URISyntaxException {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "iteration_" + iteration);

		job.setJarByClass(KmeansnD.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(KmeansnDCombinedWritable.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(KmeansnDMapper.class);
		job.setCombinerClass(KmeansnDCombiner.class);
		job.setReducerClass(kmeansnDReducer.class);

		job.setNumReduceTasks(1);
		job.addCacheFile(new URI(pivot_path));
		return job;
	}

	public void SequenceFileToText(String input_p, String output_p, int k)
			throws URISyntaxException, IOException {

		URI input_uri = new URI(input_p);
		URI output_uri = new URI(output_p);

		Configuration conf = getConf();
		FileSystem hdfs = FileSystem.get(input_uri, conf);

		OutputStream output = hdfs.create(new Path(output_uri));

		SequenceFile.Reader reader = null;
		reader = new SequenceFile.Reader(conf, Reader.file(new Path(input_p)));

		NullWritable null_w = NullWritable.get();
		KmeansnDCombinedWritable combined_w = new KmeansnDCombinedWritable();

		int i = 0;
		while (reader.next(null_w, combined_w)) {
			output.write((i + "\t").getBytes());

			for (int j = 0; j < combined_w.getCoordinates().size(); j++) {
				output.write((Double.toString(combined_w.getCoordinates()
						.get(j))).getBytes());
				if (j != combined_w.getCoordinates().size() - 1)
					output.write(",".getBytes());
			}
			output.write("\n".getBytes());
			++i;
		}

		reader.close();
		output.close();
	}

	public void createPivots(String input_p, String output_p, int k)
			throws URISyntaxException, IOException {

		URI input_uri = new URI(input_p);
		URI output_uri = new URI(output_p);

		Configuration conf = getConf();
		FileSystem hdfs = FileSystem.get(input_uri, conf);

		BufferedReader reader = new BufferedReader(new InputStreamReader(
				hdfs.open(new Path(input_uri))));
		OutputStream output = hdfs.create(new Path(output_uri));

		String line;
		int i = 0;
		StringBuilder sb = new StringBuilder();

		while (i < k && (line = reader.readLine()) != null) {
			try {
				String[] splits = line.split(",");
				sb.setLength(0);
				int nb_col = conf.getInt("pivots.dimension", 0);
				sb.append(i + "	");
				for (int j = 0; j < nb_col; j++) {
					int col = conf.getInt("pivots.column_number." + j, 0);
					// Check if splits[col] is a valid double
					Double.parseDouble(splits[col]);
					sb.append(splits[col]);
					if (j != nb_col - 1) {
						sb.append(",");
					}
				}
				sb.append("\n");
				output.write(sb.toString().getBytes());

				i++;
			} catch (NumberFormatException e) {

			}
		}
		reader.close();
		output.close();
	}

	private void copyAsSequenceFile(Path input, Path output)
			throws IllegalArgumentException, IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf);
		job.setJobName("Convert to SequenceFile");
		job.setJarByClass(KmeansnD.class);

		job.setMapperClass(kmeansnD.MapperCopyToSequenceFile.class);

		job.setNumReduceTasks(0);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(KmeansnDCombinedWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		TextInputFormat.addInputPath(job, input);
		SequenceFileOutputFormat.setOutputPath(job, output);

		job.waitForCompletion(false);
	}

	private void createNewClustersFile(Path input_path, Path output_path,
			Path pivot_path, int k) throws IOException, URISyntaxException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf);

		job.setJarByClass(Kmeans.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(KmeansnDCombinedWritable.class);

		job.setMapperClass(kmeansndCreateClustersMapper.class);

		TextInputFormat.addInputPath(job, input_path);
		for (int i = 0; i < k; ++i) {
			MultipleOutputs.addNamedOutput(job, Integer.toString(i),
					SequenceFileOutputFormat.class, NullWritable.class,
					KmeansnDCombinedWritable.class);
		}
		TextOutputFormat.setOutputPath(job, output_path);

		job.setNumReduceTasks(1);
		job.addCacheFile(new URI(pivot_path.toString()));
		job.getConfiguration().set("pivots.uri", pivot_path.toString());

		job.waitForCompletion(true);
	}

	public static void main(String args[]) throws Exception {
		System.exit(ToolRunner.run(new KmeansnDHierar(), args));
	}
}
