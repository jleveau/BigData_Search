package pivots;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PivotsSequenced extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		System.out.println("in");
		Configuration conf = getConf();

		URI input_uri = new URI(args[0]);
		Path output_path = new Path("/resources/pivots");
		
		Integer k = new Integer(args[1]);

		Job job = Job.getInstance(conf, "Kmeans");

		FileSystem hdfs = FileSystem.get(input_uri, conf);

		BufferedReader reader = new BufferedReader(new InputStreamReader(
				hdfs.open(new Path(input_uri))));

		String line;
		// Skip the first line
		reader.readLine();
		int i = 0;
		IntWritable key = new IntWritable();
		Text value = new Text();
	
		SequenceFile.Writer writer = SequenceFile.createWriter(conf,
				Writer.file(output_path), Writer.keyClass(IntWritable.class),
				Writer.valueClass(Text.class));
		
		while (i < k && (line = reader.readLine()) != null) {
			String[] splits = line.split(",");
			
			try {
				// check if value can be cast to Double
				double pop = Double.parseDouble(splits[4]);
				i++;
				key.set(i);
				value.set(line);
				writer.append(key, value);
			} catch (NumberFormatException e) {

			}

		}

		IOUtils.closeStream(writer);
		reader.close();

		return 0;
	}

	public static void main(String args[]) throws Exception {
		// Take 3 parameters : input output k
		System.exit(ToolRunner.run(new PivotsSequenced(), args));
	}

}
