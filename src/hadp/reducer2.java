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
import org.apache.hadoop.mapreduce.Reducer.Context;

public class reducer2 extends Reducer<Text,IntWritable,Text,IntWritable> {
	Map<String, Integer> maptemp = new HashMap<String, Integer>();
	String wikitype=null;
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws
	IOException, InterruptedException
	{
		int Count = 0;
		
		for (IntWritable val : values)
		{
			String tokens[]=val.toString().trim().split(":");
			Count+=val.get();
		}
		switch(key.toString())
		{
		  case "b":
			     wikitype="WikiBooks";
		          break;
		  case "d":
			      wikitype="Wikionary";
		          break;
		  case "m":
			     wikitype="WikiMedia";
		          break;
		  case "mw":
			      wikitype="Wikipedia Mobile";
		          break;
		  case "n":
			     wikitype="WikiNews";
		          break;
		  case "q":
			      wikitype="WikiQuote";
		          break;
		  case "s":
			     wikitype="WikiSource";
		          break;
		  case "v":
			      wikitype="WikiVersity";
		          break;
		  case "w":
			     wikitype="MediaWiki";
		          break;
		}
		
		maptemp.put(wikitype,Count);
	}
	
	
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		
		//context.write(new Text("Report of all Wiki Type viewcount"),null);
        context.write(new Text("*********************************"),null);
		
		context.write(new Text("\t\t\tREPORT OF ALL WIKI TYPE AS PER VIEW COUNT\t\t\t"),null);
		context.write(new Text("*********************************"),null);
		context.write(new Text("WIKI TYPE \t\tVIEW COUNT\t\t"),null);
		context.write(new Text("--------------------------------------------"),null);
		
        context.write(new Text("*********************************"),null);
		
		context.write(new Text("\t\t\tMOSTLY USED WIKI TYPE \t\t\t"),null);
		context.write(new Text("*********************************"),null);
			
		Map.Entry<String, Integer> maxEntry = null;

			for(Entry<String, Integer> entry : maptemp.entrySet())
			{
				if(maxEntry == null || entry.getValue().compareTo(maxEntry.getValue()) > 0)
					maxEntry = entry;
				context.write(new Text(entry.getKey()+"  "),new IntWritable(entry.getValue()));
			    
			}
			context.write(new Text("Which Wiki type mostly used"+maxEntry.getKey()),new IntWritable(maxEntry.getValue()));
	    
	
	}
	
}