package hw3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * Created by Shabbir Saifee.
 * purpose: Contains information about the node/page i.e page name, its out links, page rank
 */
// 
public class Page {

	
	String pageName;
	double pageRank;
	boolean isNeighbour;
	List<String> adjacenctPages = new  ArrayList<String>();
	
	public Page() {
		pageName="";
		pageRank=-1.0;
		isNeighbour=false;
		adjacenctPages = new  ArrayList<String>();
	}
	
	Page(String pageName,List<String> adjacencyList){
		this.pageName = pageName;
		this.adjacenctPages=adjacencyList;
	}

	/**
	 * This function returns a new page object
	 * @param value
	 * @return
	 * @throws IOException
	 */
	public static Page stringToNode(String line) throws IOException {
		 
		 String pageName = line.substring(0, line.indexOf("[")-1);
		 String outLinks = line.substring(line.indexOf("[")+1,line.indexOf("]"));
		 //String isNeighbour = line.substring(line.indexOf("(")+1,line.indexOf(")"));
		 String list[];
		 List <String> adjList = new ArrayList<String>();
		 if(outLinks.equals("")) {
			 
		 }
		 else {
			 list = outLinks.split(", ");
			  adjList = Arrays.asList(list);
		 }
		Double pageRank = Double.parseDouble(line.substring(line.indexOf('{')+1,line.indexOf('}')));
		Page node = new Page();
		node.pageName=pageName;
		node.pageRank=pageRank;
		if(line.contains("(ISNEIGHBOUR)")) {
			node.isNeighbour=true;
		}
		else {
			node.isNeighbour=false;
		}
		node.adjacenctPages=adjList;
		return node;
}
	
    public String toString(boolean isNeighbour){
        StringBuilder sb = new StringBuilder();

        
        if (this.adjacenctPages != null && this.adjacenctPages.size()>0) {
            sb.append("[");
                sb.append(StringUtils.join (this.adjacenctPages, ", "));
            sb.append("]");
        }
        else {
        	sb.append("[]");
        }
        sb.append(" {" + this.pageRank + "}");
        if(isNeighbour) {
        	sb.append(" (ISNEIGHBOUR)");
        }
        return sb.toString();
    }
    

	
}
