import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class NGramLibraryBuilder {
	public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable>{		
		 private final static IntWritable one = new IntWritable(1);
		 private int nGram;
		 @Override
			public void setup(Context context) {
				Configuration conf = context.getConfiguration();
				nGram = conf.getInt("noGrams", 5);
			}
		 
		 
		 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			 String line=value.toString();
			 line=line.trim().toLowerCase();
			 line = line.replaceAll("[^a-z]+", " ");
			 String[] words=line.split("\\s+");
			 StringBuilder sb=new StringBuilder();
			 if (words.length<2)
			 {return;}
			 for (int i=0;i<words.length-nGram;i++)
			 {
				 sb = new StringBuilder();
				 for (int j=0;j<nGram;j++)
				 {
					 sb.append(" ");
					 sb.append(words[i+j]);
					 String keyOutPut=sb.toString().trim();
					 if (keyOutPut.length()>0)
					 {context.write(new Text(keyOutPut), one);}
				 }
			 }
		 }
	}
		
		public static class NGramReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		 private IntWritable result = new IntWritable();
		
		 public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		   int sum = 0;
		   for (IntWritable val : values) {
		     sum += val.get();
		   }
		   result.set(sum);
		   context.write(key, result);
		 }
		}
}
