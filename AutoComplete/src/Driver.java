import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		 Configuration conf1= new Configuration();
		 conf1.set("noGrams", args[3]);
		 Job job1 = Job.getInstance(conf1);
		 job1.setJobName("NGrams");
		 job1.setJarByClass(Driver.class);
		 job1.setMapperClass(NGramLibraryBuilder.NGramMapper.class);
		 job1.setReducerClass(NGramLibraryBuilder.NGramReducer.class);
		 job1.setOutputKeyClass(Text.class);
		 job1.setOutputValueClass(IntWritable.class);
		 job1.setInputFormatClass(TextInputFormat.class);
		 job1.setOutputFormatClass(TextOutputFormat.class);
		 TextInputFormat.addInputPath(job1, new Path(args[0]));
		 TextOutputFormat.setOutputPath(job1, new Path(args[1]));
		 job1.waitForCompletion(true);
		 
		 Configuration conf2= new Configuration();
		 conf2.set("threshold", args[4]);
		 DBConfiguration.configureDB(conf2,
	    	     "com.postgresql.jdbc.Driver",   // driver class
	    	     "jdbc:postgresql://localhost:5432/autocomplete", //
	    	     "",    // user name
	    	     "Huang19940727"); //password
		 Job job2 = Job.getInstance(conf2);
		 job2.setJobName("LanguageModel");
		 job2.setJarByClass(Driver.class);
		 job2.setMapperClass(LanguageModel.LanguageModelMapper.class);
		 job2.setReducerClass(LanguageModel.LanguageModelReducer.class);
		 job2.setOutputKeyClass(Text.class);
		 job2.setOutputValueClass(IntWritable.class);
		 job2.setInputFormatClass(TextInputFormat.class);
		 job2.setOutputFormatClass(TextOutputFormat.class);
		 TextInputFormat.addInputPath(job2, new Path(args[1]));
		 DBOutputFormat.setOutput(
			     job2,
			     "output",    // output table name
			     new String[] { "phrase","count" }   //table columns
			     );
		 System.exit(job2.waitForCompletion(true)?0:1);
	}
}
