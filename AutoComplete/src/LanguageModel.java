import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class LanguageModel {
	public static class LanguageModelMapper extends Mapper<LongWritable, Text, Text, IntWritable>{		
		 private int threshold;
		 @Override
		 public void setup(Context context) {
			 Configuration conf = context.getConfiguration();
			 threshold = conf.getInt("threshold", 5);
		}
			 
			 
		 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] wordsPlusCount=value.toString().split("\t");
			int count=Integer.parseInt(wordsPlusCount[wordsPlusCount.length-1]);
			String words=wordsPlusCount[0];
			if (words.length()>0&&count>threshold)
			{context.write(new Text(words),new IntWritable(count));};
		}
	}
		
		public static class LanguageModelReducer extends Reducer<Text,IntWritable,DBOutputWritable,NullWritable> {
			
		 public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			 for (IntWritable value:values)
			 {
				 context.write(new DBOutputWritable(key.toString(),value.get()),NullWritable.get());
			 }
			 
		 }
	}
}
