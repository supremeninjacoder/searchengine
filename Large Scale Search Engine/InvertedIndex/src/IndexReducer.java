import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Iterator;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import java.util.*;

public class IndexReducer extends Reducer<Text, Text, Text, ArrayListWritable<LongWritable>> {
    
    private Text result = new Text();
 
    public void reduce(Text key, Iterable<Text> values, Context context)
              throws IOException, InterruptedException {

	Iterator<Text> iter = values.iterator();
	ArrayList<Long> docIds = new ArrayList<Long>();
	ArrayListWritable<LongWritable> listOfDocIds = new ArrayListWritable<LongWritable>();

	while (iter.hasNext()) {
		Long no = Long.parseLong(iter.next().toString());
		docIds.add(no);
	}

	//Collections.sort(docIds);
	for(Long docId : docIds){
		listOfDocIds.add(new LongWritable(docId));
	}
	context.write(key,listOfDocIds);
    }
}
