package pagerank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class ComputeScore {
	
	/*The class implemented to iteratively calculate the page rank score*/
	public static class ComputeScoreMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			/*
			 * The mapper input <Node	score	outNode1,outNode2,outNode3,бнбн,outNodeN>
			 * The mapper output <Node #outNode1,outNode2,бнбн,node,outNodeN> # sign is used to identify node and score for reducers
			 * 					<#outNode1,1/N> <#outNode2,1/N>,бнбн,<#outNodeN,1/N>
			 * 
			 * */
			String[] split= value.toString().split("\t");
			Double score=Double.valueOf(split[1]);
			String[] outNodes=split.length>=3?split[2].split(PageRank.LINKS_SEPARATOR):new String[0];
			int outDegree=outNodes.length;
			String node=split[0];
			//dead end node, keep the score
			if (outNodes.length==0)
			{
				context.write(new Text(node),new Text(String.valueOf(score)));
			}
			//else the origin page rank score is spread evenly to all the out nodes
			for (String outNode:outNodes)
			{
				context.write(new Text(outNode),new Text(String.valueOf(score/outDegree)));
			}
			//output the out nodes to remember the graph structure
			context.write(new Text(node),new Text("#"+(split.length>=3?split[2]:"")));
			
		}
	}
	
	public static class ComputeScoreReducer extends Reducer<Text,Text,Text,Text> {
	
	 	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	 		
	 		Double score=(1.0-PageRank.DAMPING)/PageRank.NODES.size();
	 		String links="";
	 		for (Text value:values)
	 		{
	 			if (value.toString().startsWith("#")) // if it is out nodes information
	 			{
	 				links=value.toString().substring(1);
	 			}
	 			else								// else if it is a part of rank score
	 			{
	 				score+=PageRank.DAMPING*Double.valueOf(value.toString());
	 			}
	 			
	 		}
	 		context.write(new Text(key), new Text(score+"\t"+links));
	 	}
	}
}
