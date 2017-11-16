package hw3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * Created by Shabbir Saifee
 * Purpose: Generates key(page name) and value (outlinks and pagerank)
 */
public class PreprocessReducer extends Reducer<Text, Text, Text, Text> {
	
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String output = new String();

        for (Text str : values) {
            output += str.toString();
        }

        if (output.equals(""))
            output = "[]"; 

        // incrementing total number of nodes in the graph
        context.getCounter(PageRankDriver.NODE_COUNTER.totalNodes).increment(1);

        // adding dummy value for page rank
        output = output + " {-1.0}";

        context.write(key, new Text (output));
    }
}

