package pagerank;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import pagerank.PageRank;
public class ParsingGraph {
	public static class ParsingGraphMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			/*
			 * input of mapper <outNode inNode>
			 * output of mapper <outNode, inNode>,<inNode,"">
			 * 
			 * */
			String[] inAndOut=value.toString().split("\\s+");
			String in=inAndOut[0];
			String out=inAndOut[1];
			context.write(new Text(in),new Text(out));
			context.write(new Text(out),new Text(""));
			PageRank.NODES.add(in);	// add the node to the hashSet NODES to count 
			PageRank.NODES.add(out);
		}
	}
	
	public static class ParsingGraphReducer extends Reducer<Text,Text,Text,Text> {
		/*
		 * reducer input <innode,<outNode1,outNode2,¡­¡­,outNodeN>>
		 * reducer output<innode,<1/N	outNode1,outNode2,outNode3,¡­¡­outNodeN>>
		 * 

		 * */
	 	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	 		Double initialRank=1.0/PageRank.NODES.size();//initial ranks to 1/N
	 		StringBuilder sb=new StringBuilder();
	 		sb.append(initialRank);
	 		sb.append("\t");
	 		for (Text value:values)
	 		{
	 			if (value.toString().length()>0)
	 			{
	 				sb.append(value);
	 				sb.append(PageRank.LINKS_SEPARATOR);
	 			}
	 		}
	 		context.write(new Text(key), new Text(sb.toString()));
	 	}
	}
}
