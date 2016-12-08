package hadp;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class reducefn extends Reducer<Text,Text,Text,Text> {

	private static final int String = 0;
	Map<String,Integer> outrmp = new HashMap<String,Integer>();
	Map<String, Integer> pagebucket = new HashMap<String, Integer>();
	Map<String, String> mnthpg = new HashMap<String, String>();
	Map<String,BigInteger> pagesizelist = new HashMap<String,BigInteger>();
	
	IntWritable result = new IntWritable();
	static String highPageName=null;

	public void reduce(Text key, Iterable<Text> values, Context context) throws
		IOException, InterruptedException
	{
		
		int viewCount = 0;
		BigInteger pageSize = null;
		
		for (Text val : values)
		{
			String tokens[]=val.toString().trim().split(":");
			viewCount=Integer.parseInt(tokens[0]);
			pageSize=new BigInteger(tokens[1]);
			
		}
		result.set(viewCount);
		pagebucket.put(key.toString(), viewCount);
		pagesizelist.put(key.toString(), pageSize);
	}


	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		outrmp.put("JANUARY",0);
		outrmp.put("FEBRUARY",0);
		outrmp.put("MARCH",0);
		outrmp.put("APRIL",0);
		outrmp.put("MAY",0);
		outrmp.put("JUNE",0);
		outrmp.put("JULY",0);
		outrmp.put("AUGUST",0);
		outrmp.put("SEPTEMBER",0);
		outrmp.put("OCTOBER",0);
		outrmp.put("NOVEMBER",0);
		outrmp.put("DECEMBER",0);
		
		mnthpg.put("JANUARY",null);
		mnthpg.put("FEBRUARY",null);
		mnthpg.put("MARCH",null);
		mnthpg.put("APRIL",null);
		mnthpg.put("MAY",null);
		mnthpg.put("JUNE",null);
		mnthpg.put("JULY",null);
		mnthpg.put("AUGUST",null);
		mnthpg.put("SEPTEMBER",null);
		mnthpg.put("OCTOBER",null);
		mnthpg.put("NOVEMBER",null);
		mnthpg.put("DECEMBER",null);
		
		// TODO Auto-generated method stub
		super.cleanup(context);
		
		
		
		for(Entry<String, Integer> entry : pagebucket.entrySet())
		{
			String keytemp[] = entry.getKey().toString().split(":");
			
			String month = keytemp[0];
			String pageName = keytemp[1];
			int visitCount = entry.getValue();
			if(outrmp.get(month)<visitCount)
			{
				mnthpg.put(month, pageName);
				outrmp.put(month, visitCount);
			}
			
		}
		
		context.write(new Text("*********************************"),null);
		
		context.write(new Text("\t\t\tHIGHEST VIEWCOUNT IN EACH MONTH\t\t\t"),null);
		context.write(new Text("*********************************"),null);
		context.write(new Text("MONTH NAME \t\tPAGE NAME\t\tVIEW COUNT\t\t"),null);
		context.write(new Text("--------------------------------------------"),null);
		
		for(Entry<String, Integer> ent : outrmp.entrySet())
		{
			context.write(new Text(ent.getKey()+"  "+mnthpg.get(ent.getKey())+"  "),new Text(ent.getValue().toString()));
		}	
		
         context.write(new Text("*********************************"),null);
		
		context.write(new Text("\t\t\tHIGHEST VIEWCOUNT IN WHOLE YEAR\t\t\t"),null);
		context.write(new Text("*********************************"),null);
		
	Map.Entry<String, Integer> maxEntry = null;

		for(Entry<String, Integer> entry : pagebucket.entrySet())
		{
			if(maxEntry == null || entry.getValue().compareTo(maxEntry.getValue()) > 0)
				maxEntry = entry;
		}
		context.write(new Text("Yearly trend"+maxEntry.getKey()),new Text(maxEntry.getValue().toString()));
    
		
		Map.Entry<String, BigInteger> maxEntry1 = null;

		for(Entry<String, BigInteger> entry1 : pagesizelist.entrySet())
		{
			if(maxEntry1 == null || entry1.getValue().compareTo(maxEntry1.getValue()) > 0)
				maxEntry1 = entry1;
		}
		 context.write(new Text("*********************************"),null);
			
			context.write(new Text("\t\t\tMOST INFORMATIVE PAGE OF THE YEAR\t\t\t"),null);
			context.write(new Text("*********************************"),null);
			
		context.write(new Text("Most Informative Page "+maxEntry1.getKey()),new Text(maxEntry1.getValue().toString()));
		
		
		
	}//end of cleanup
	

	
	
	}//end of class
