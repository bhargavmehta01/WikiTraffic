package hadp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class wordcnt{
	public static void main(String args[]) throws Exception{
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "wiki traffic");
	    job.setJarByClass(wordcnt.class);
	    job.setMapperClass(mapdemo.class);
	    job.setReducerClass(reducefn.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	   // System.exit(job.waitForCompletion(true) ? 0 : 1);
	    job.waitForCompletion(true);
	    //JobClient.runJob(job);
	    
	    Job job2 = Job.getInstance(conf,"wiki pages");
	    job2.setJarByClass(wordcnt.class);
	    job2.setMapperClass(mapwiki.class);
	    job2.setReducerClass(reducer2.class);
	    job2.setMapOutputKeyClass(Text.class);
	    job2.setMapOutputValueClass(IntWritable.class);
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job2, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
	    System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
