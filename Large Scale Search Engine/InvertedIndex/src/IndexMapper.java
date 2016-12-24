import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IndexMapper extends Mapper<LongWritable, Text, Text, Text>{

	private static Text word = new Text();

	public void map(LongWritable position, Text value,Context context) throws IOException, InterruptedException {
		
        
		String fields[] = value.toString().split("\t");
		if(fields.length == 2){
			try { 
				Long.parseLong(fields[0]); 
				List<String> uniqueWords = new ArrayList<String>();
				StringTokenizer itr = new StringTokenizer(fields[1].toLowerCase(), "-- \t\n\r\f,.:;?![]'\"");
				while (itr.hasMoreTokens()) {
					String token = itr.nextToken();
					if (token == null || token.length() == 0) {
						continue;
					}

					if (!(uniqueWords.contains (token))){
						uniqueWords.add(token);
					}
				}
				for(int i=0;i<uniqueWords.size();i++){
					word.set(uniqueWords.get(i));
					context.write(word,new Text(fields[0]));
				}
			} 
			
			catch(NumberFormatException e) { 

			}

			
		}
	}

}
