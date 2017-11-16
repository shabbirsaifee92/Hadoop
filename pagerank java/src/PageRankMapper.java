package hw3;

import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.xml.sax.SAXException;
/*
 *  Created by : Shabbir Saifee
 *  Purpose: Reads each record and emits the key (page name) , value (Page)
 */
public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text>{

	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
			
		//Get the totalnumber of nodes
		int totalNodes = Integer.parseInt(context.getConfiguration().get("totalNodes"));

		 Page node = Page.stringToNode(value.toString());
		 
		 // Set initial pagerank
		 if(node.pageRank == -1.0) {
			 double d =1.0/totalNodes;
			 node.pageRank=d;
		 }
		  		 
		 context.write(new Text(node.pageName), new Text(node.toString(false)));
		 
		 Double contr;
		 if(node.adjacenctPages!=null || node.adjacenctPages.size()!=0) {
			 contr = node.pageRank/node.adjacenctPages.size();
		
			 for(String s : node.adjacenctPages) {
			 
			 Page adjNode = new Page();
			 adjNode.pageRank=contr;
			 adjNode.pageName =s;
			 adjNode.isNeighbour = true;
			 context.write(new Text(s), new Text(adjNode.toString(true)));
			 
			 }
		 }		
		 
		 		
	}
	
	
}
