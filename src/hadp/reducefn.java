package hadp;

import java.io.IOException;
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


public class reducefn extends Reducer<Text,IntWritable,Text,IntWritable> {

	private static final int String = 0;
	Map<String,Integer> outrmp = new HashMap<String,Integer>();
	Map<String, Integer> pagebucket = new HashMap<String, Integer>();
	Map<String, String> mnthpg = new HashMap<String, String>();
	
	IntWritable result = new IntWritable();
	static String highPageName=null;

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws
		IOException, InterruptedException
	{
		int sum = 0;
		
		for (IntWritable val : values)
		{
			sum = val.get();
		}
		result.set(sum);
		pagebucket.put(key.toString(), sum);
		//context.write(new Text(key),result);
	}

	//context.write(new Text("Highest is : "), new Text(statesbucket.toString()));

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
		
		
		for(Entry<String, Integer> ent : outrmp.entrySet())
		{
			result.set(ent.getValue());
			context.write(new Text(ent.getKey()+mnthpg.get(ent.getKey())),result);
		}
				
				/*Map.Entry<String, Integer> maxEntry = null;
				for(Entry<String, Integer> entr2 : inrmp.entrySet())
				{
					if(maxEntry == null || entr2.getValue().compareTo(maxEntry.getValue()) > 0)
						maxEntry = entr2;
				}
				result.set(maxEntry.getValue());
				outrmp.put(month, inrmp);
			
		}
		
		Set sts = outrmp.entrySet();*/
			
			
		
		
		
		

		
	Map.Entry<String, Integer> maxEntry = null;

		for(Entry<String, Integer> entry1 : pagebucket.entrySet())
		{
			if(maxEntry == null || entry1.getValue().compareTo(maxEntry.getValue()) > 0)
				maxEntry = entry1;
		}
		result.set(maxEntry.getValue());
		context.write(new Text("Yearly trend"+maxEntry.getKey()),result);
    
	}
	}
