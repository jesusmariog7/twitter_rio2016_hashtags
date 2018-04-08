import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OlympicMapperB_Hashtags extends Mapper<Object, Text, Text, IntWritable> {

	private Text data = new Text();
	private int mostPopularHour = 22; //format without minutes
	private final IntWritable one = new IntWritable(1);
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		
    	String line = value.toString();
    	
    	if(line.contains(";")){
    		try {
    		String tweetParts[] = line.split(";");
    		
    			if(tweetParts.length == 4) {
	            
			    	String tweet = tweetParts[2];
					long epoch_time = Long.parseLong(tweetParts[0]);
			    	
			    	// only process tweets less or equal to 140 chars
			    	if(tweet.length()>= 1 && tweet.length()<= 140) {
			    			
			    		//Substring the hour from epoch_time
			    		String time = (LocalDateTime.ofEpochSecond(epoch_time/1000,0, ZoneOffset.ofHours(-3))).toString();
			    		String hour = time.substring(time.indexOf('T')+1);
			    		int hourInt = Integer.parseInt(hour.substring(0,2));	
			    		
			    		//Process only hashtags from tweets in the top hour
			    		if(hourInt == mostPopularHour) {
			    			
			    			if(tweet.contains("#")) { 				
			    				tweet = tweet.toLowerCase();
			    				
				    			Pattern hashtag_pattern = Pattern.compile("#(\\w+)");
				    			Matcher match = hashtag_pattern.matcher(tweet);
				    			
				    			while (match.find()) {
				    				String hashtagValue = match.group(1);
				    				data.set("#" + hashtagValue);
				    				context.write(data, one);
				    			}
			    			}
			    		}   		
			    	}	
    			}
    		} catch(Exception e) {}
    	}
	}
}
