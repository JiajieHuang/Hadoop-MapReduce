package pagerank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RankOrdering {
	//just used to rank the page scores.
	public static class RankOrderingMapper extends Mapper<LongWritable, Text, DoubleWritable, Text>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] split=value.toString().split("\t");
			double score=Double.parseDouble(split[1]);
			String node=split[0];
			context.write(new DoubleWritable(score),new Text(node));			
		}
	}
}
