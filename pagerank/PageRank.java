package pagerank;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PageRank {
	public static double DAMPING=0.85;
	public static int ITERATION=2;
	public static Set<String> NODES=new HashSet<String>();
	public static String LINKS_SEPARATOR=",";
	public static void main(String[] args) throws IllegalArgumentException, ClassNotFoundException, IOException, InterruptedException {
		String input=args[0];//input file path, stores information of all links
		String output=args[1]; // output file path, stores information of page names and its pagerank
		boolean isCompleted=job1(input,output+"/iter0");
		if (!isCompleted)
		{
			System.exit(1);
		}
		for (int i=0;i<ITERATION;i++)
		{
			isCompleted=job2(output+"/iter"+(i),output+"/iter"+(i+1));
			if (!isCompleted)
			{
				System.exit(1);
			}
		}
		isCompleted = job3(output+"/iter"+ITERATION, output + "/result");
        if (!isCompleted) {
            System.exit(1);
        }
        System.out.println("DONE!");	
	}
	/*job1 parse the graph*/
	private static boolean job1(String input, String output) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "parsingGraph");
	    job.setJarByClass(PageRank.class);
	    job.setMapperClass(ParsingGraph.ParsingGraphMapper.class);
	    job.setReducerClass(ParsingGraph.ParsingGraphReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    return job.waitForCompletion(true);
	}
	/*job two does one iteration to count page rank*/
	private static boolean job2(String input, String output) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "computeScore");
	    job.setJarByClass(PageRank.class);
	    job.setMapperClass(ComputeScore.ComputeScoreMapper.class);
	    job.setReducerClass(ComputeScore.ComputeScoreReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    return job.waitForCompletion(true);
	}
	/*job 3 output the final sorted pagerank score*/
	private static boolean job3(String input, String output) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "RankOrdering");
	    job.setJarByClass(PageRank.class);
	    job.setMapperClass(RankOrdering.RankOrderingMapper.class);
	    job.setOutputKeyClass(DoubleWritable.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    return job.waitForCompletion(true);
	}

}
