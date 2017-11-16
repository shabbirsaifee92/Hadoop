package hw3;
import java.io.IOException;

import javax.sound.midi.Sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * Created By shabbir saifee
 * 
 */

public class PageRankDriver {

	static double delta=0.0;
	static long startTime;
	static long endTime;
	static long startTime1;
	static long endTime1;
	static long startTime2;
	static long endTime2;
	
    public enum NODE_COUNTER {
        totalNodes,delta;
    }
    
    static int totalNodes;
	public static void main(String[] args) throws IOException,Exception {
		
		Configuration conf = new Configuration();
		conf.setInt("totalNodes", 0);
//		
		//This is the pre-processing job
		Job preprocess = Job.getInstance(conf,"PageRank");
		
		preprocess.setJarByClass(PageRankDriver.class);
		
		//Set Mapper and Reducer Classes
		preprocess.setMapperClass(PreprocessMapper.class);
		preprocess.setReducerClass(PreprocessReducer.class);
		preprocess.setNumReduceTasks(1);
		
		preprocess.setMapOutputKeyClass(Text.class);
		preprocess.setMapOutputValueClass(Text.class);
		preprocess.setOutputKeyClass(Text.class);
		preprocess.setOutputValueClass(Text.class);
		for(int i =0;i<4;i++) {
			MultipleInputs.addInputPath(preprocess, new Path(args[0]+"."+i+""), TextInputFormat.class);
		}
		
		//FileInputFormat.setInputPaths(preprocess, new Path(args[0]));
		FileOutputFormat.setOutputPath(preprocess, new Path(args[1]+"0"));
		
		startTime = System.currentTimeMillis();
		preprocess.waitForCompletion(true);
		endTime = System.currentTimeMillis();
		System.out.println("Preproessing took "+(endTime-startTime)/1000 + " seconds");
		//Total number of nodes after the pre-processing
		totalNodes = (int) preprocess.getCounters().findCounter(NODE_COUNTER.totalNodes).getValue();
//		

		//Running 10 iterations to calculate page rank
		startTime1 = System.currentTimeMillis();
		for(int i =1;i<=10;i++) {
			
			
            conf.setInt("totalNodes", (int)totalNodes);
            conf.setInt("itr", i);
            conf.setDouble("delta", delta);
            
            Job job = Job.getInstance(conf, "PageRankCalculation");

            job.setJarByClass(PageRankDriver.class);
            job.setNumReduceTasks(1);
            
            job.setMapperClass(PageRankMapper.class);
            job.setReducerClass(PageRankReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[1]+(i-1)+"/part-r-00000" ));
            FileOutputFormat.setOutputPath(job, new Path(args[1]+i));
            
            job.waitForCompletion(true);
            endTime1 = System.currentTimeMillis();
            delta = (double)job.getCounters().findCounter(NODE_COUNTER.delta).getValue()/Math.pow(10, 15);
       
		}
		
		
		// topPages to find top K (100) pages
        Job topPages = Job.getInstance(conf, "Top K Pages");

        topPages.setJarByClass(PageRankDriver.class);
        topPages.setNumReduceTasks(1);
        
        topPages.setMapperClass(TopKPageRank.TopKMapper.class);
        topPages.setReducerClass(TopKPageRank.TopKReducer.class);
        topPages.setMapOutputKeyClass(CompositeKey.class);
        topPages.setMapOutputValueClass(NullWritable.class);
        topPages.setOutputKeyClass(Text.class);
        topPages.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(topPages, new Path (args[1]+"10/part-r-00000"));
        FileOutputFormat.setOutputPath(topPages, new Path(args[1]+"TopK"));
        startTime2 = System.currentTimeMillis();
        boolean i = topPages.waitForCompletion(true);
        endTime2 = System.currentTimeMillis();	
        System.out.println("Preproessing took "+(endTime-startTime)/1000 + " seconds");
        System.out.println("10 iteerations took "+(endTime1-startTime1)/1000 + " seconds");
        System.out.println("top k calculation took "+(endTime2-startTime2)/1000 + " seconds");
        System.exit(i==true? 0:1);

		
	}
}
