package RankResult;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RankingReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    public void reduce(Text key, FloatWritable value, Context context) throws IOException, InterruptedException {
        context.write(key,value);
    }
}
