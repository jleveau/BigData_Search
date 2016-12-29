package kmeansnD;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class KmeansnDCombiner extends Reducer<IntWritable,KmeansnDCombinedWritable,IntWritable, 
												KmeansnDCombinedWritable>  {

}
