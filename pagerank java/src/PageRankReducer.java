package hw3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import hw3.PageRankDriver.NODE_COUNTER;

/*
 * Created By: Shabbir Saifee
 * purpose: Takes each key(page name) and list of values (pages) and calculates the pagerank for each page
 * 
 */
public class PageRankReducer extends Reducer<Text, Text, Text, Text>{
	
	double pageRankSum = 0.0;
	double lambda = 0.85;
	int totalNodes;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		context.getCounter(PageRankDriver.NODE_COUNTER.delta).setValue(0);
		totalNodes = Integer.parseInt(context.getConfiguration().get("totalNodes"));
	}

	@Override
	protected void reduce(Text key, Iterable<Text> nodes, Context context)
			throws IOException, InterruptedException {

		int itr = context.getConfiguration().getInt("itr", 0);
		double delta = context.getConfiguration().getDouble("delta", 0.0);
		double pageRank =0.0;
		Page n = new Page();
		
		//for the first iteration
		if(itr == 1) {
			pageRank = 1.0/totalNodes;
			for(Text v : nodes) {
				Page node = Page.stringToNode(key.toString()+" "+v.toString());
				if(!node.isNeighbour) {
					n.adjacenctPages = node.adjacenctPages;
				}
			}
		}else {
			pageRank = (1- lambda)/totalNodes + lambda*delta/totalNodes;
			for(Text v: nodes ) {			
				Page node = Page.stringToNode(key.toString()+" "+v.toString());
				if(node.isNeighbour) {
					pageRank += lambda*node.pageRank;
				}
				else {				
					n.adjacenctPages = node.adjacenctPages;
				}			
				
			}
		}
						
		//If the current node is a sink node then update the delta value
		if(n.adjacenctPages.size()==0) {		
			context.getCounter(PageRankDriver.NODE_COUNTER.delta).increment((long) (pageRank*Math.pow(10, 15)));
		}
		pageRankSum += pageRank;
		
		Page node = new Page();
		node.pageName = key.toString();
		node.pageRank = pageRank;
		node.adjacenctPages = n.adjacenctPages;
		node.isNeighbour=false;
		context.write(key, new Text(node.toString(false)));
		
		
	}
	

}

