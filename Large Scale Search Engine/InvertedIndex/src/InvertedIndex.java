import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;


public class InvertedIndex {

  public static void runJob(String[] input, String output) throws Exception {

    Configuration conf = new Configuration();
    Path outputPath = new Path(output);

    Job job = new Job(conf);
    job.setJarByClass(InvertedIndex.class);
    job.setMapperClass(IndexMapper.class);
    job.setReducerClass(IndexReducer.class);
    job.setNumReduceTasks(1);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
    FileOutputFormat.setOutputPath(job, outputPath);
    outputPath.getFileSystem(conf).delete(outputPath,true);

    job.setOutputFormatClass(MapFileOutputFormat.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(ArrayListWritable.class);

    job.waitForCompletion(true);
  }

  public static void main(String[] args) throws Exception {
       runJob(Arrays.copyOfRange(args, 0, args.length-1), args[args.length-1]);
  }

}


