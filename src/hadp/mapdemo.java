package hadp;

import java.io.IOException;
import java.time.Month;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class mapdemo extends Mapper<Object, Text, Text, IntWritable>{
	/*private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();*/

	private IntWritable one;
	private Text second =new Text();    
    public void map(Object key, Text value, Context context) throws 
    	IOException, InterruptedException {
    	
    	String fname = ((FileSplit) context.getInputSplit()).getPath().getName();
    	String monthtoken[]=fname.trim().split("-");
    	String year=monthtoken[1];
    	String mon=year.substring(4,6);
    	int month=Integer.parseInt(mon);
    	String month2 = Month.of(month).name();
    	String filestr=value.toString(); 
    	//String[] months = {"random","January","February","March"};
        String array[]=filestr.trim().split("\t");
        try{
        for(int i=0;i<array.length;i++)
        {
        	if(!array[i].isEmpty())
        	{
        		String parts[]=array[i].split(" ");
                String projectName=parts[0].trim();
                String PageName=parts[1].trim();
              	int viewCount = Integer.parseInt(parts[2].trim());
           		int pageSize = Integer.parseInt(parts[3].trim());
           		one=new IntWritable(viewCount);
           		
                second.set(month2+":"+PageName);       
                context.write(second,one); 	
        	}
        	

        }
        }
        catch(Exception e)
        {
        	e.printStackTrace();
        }
        /*if(projectName.contains("."))
   		{
   			String wikiType[]=projectName.split(".");
   			
   			
   		}*/
   		
    }
}