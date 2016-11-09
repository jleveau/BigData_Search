package kmeans;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class KmeansMapper extends Mapper<LongWritable, Text, Points2DMeansWritable, Points2DMeansWritable> {


}
