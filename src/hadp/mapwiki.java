package hadp;

import java.io.IOException;
import java.time.Month;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class mapwiki extends Mapper<Object, Text, Text, IntWritable>{

	private IntWritable one;
	private Text second =new Text(); 
	
	public void map(Object key, Text value, Context context) throws 
	IOException, InterruptedException {
		
		/*String fname = ((FileSplit) context.getInputSplit()).getPath().getName();
    	String monthtoken[]=fname.trim().split("-");
    	String year=monthtoken[1];
    	String mon=year.substring(4,6);
    	int month=Integer.parseInt(mon);
    	String month2 = Month.of(month).name();*/
    	String filestr=value.toString(); 
    	//String[] months = {"random","January","February","March"};
        String array[]=filestr.trim().split("\t");
        try{
        for(int i=0;i<array.length;i++)
        {
        	if(!array[i].isEmpty())
        	{
        		String parts[]=array[i].split(" ");
        		//context.write(new Text(parts[i].toString()),new IntWritable(1)); 	
                String projectName=parts[i].trim();
                if(projectName.contains(".")){
                	String tokens[] = projectName.split("\\.");
                	String pagetype = tokens[1].trim();
                	
                	context.write(new Text(pagetype.toString()),new IntWritable(1)); 	
                }
               /* String PageName=parts[1].trim();
              	String viewCount = parts[2].trim();
           		String pageSize = parts[3].trim();*/
           		
           		
               // second.set(+":"+PageName);       
                
        	}
        	

        }
        }
        catch(Exception e)
        {
        	e.printStackTrace();
        }
	}
}