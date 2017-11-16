package hw3;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Created by Shabbir Saifee on 21/02/17.
 * purpose: Finds the K pages with highest pagerank sorted in descending order
 */
public class TopKPageRank {

	
    public static class TopKMapper
            extends Mapper<Object, Text, CompositeKey, NullWritable> {


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
        	String line = value.toString();
        	String pageName = line.substring(0,line.indexOf("[")-1);
        	double pageRank = Double.parseDouble(line.substring(line.indexOf("{")+1, line.indexOf("}")));
        	CompositeKey ck = new CompositeKey(pageName, pageRank);
        	context.write(ck, NullWritable.get());
        	
        }

    }

    public static class TopKReducer
            extends Reducer<CompositeKey, NullWritable, Text, NullWritable> {

    	int c =0;
        public void reduce(CompositeKey key, Iterable<NullWritable> values,
                           Context context) throws IOException, InterruptedException {
        	
        	context.write(new Text(key.toString()), NullWritable.get());
        }
        // Override the run()
        
        @Override
        public void run(Context context) throws IOException, InterruptedException {
          setup(context);
          int count = 0;
          while(context.nextKey() && count++ < 100) {
        	  reduce(context.getCurrentKey(), context.getValues(), context);
          }
        	  
          cleanup(context);
        }
      }

}