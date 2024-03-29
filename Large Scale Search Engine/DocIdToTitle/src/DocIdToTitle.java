
package edu.umd.cloud9.collection.wikipedia;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Tool for taking a Wikipedia XML dump file and spits out articles in a flat text file (article
 * Document Id and content, separated by a tab).
 *
 */
public class DocIdToTitle extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(DocIdToTitle.class);

  private static enum PageTypes {
    TOTAL, REDIRECT, DISAMBIGUATION, EMPTY, ARTICLE, STUB, OTHER
  };

  private static class MyMapper extends Mapper<LongWritable, WikipediaPage, Text, Text> {
    private static final Text articleDocId = new Text();
    private static final Text articleContent = new Text();

    @Override
    public void map(LongWritable key, WikipediaPage p, Context context)
        throws IOException, InterruptedException {
      context.getCounter(PageTypes.TOTAL).increment(1);

      if (p.isRedirect()) {
        context.getCounter(PageTypes.REDIRECT).increment(1);
      } else if (p.isDisambiguation()) {
        context.getCounter(PageTypes.DISAMBIGUATION).increment(1);
      } else if (p.isEmpty()) {
        context.getCounter(PageTypes.EMPTY).increment(1);
      } else if (p.isArticle()) {
        context.getCounter(PageTypes.ARTICLE).increment(1);

        if (p.isStub()) {
          context.getCounter(PageTypes.STUB).increment(1);
        }

        articleDocId.set(p.getDocid());
        articleContent.set(p.getTitle());

        context.write(articleDocId, articleContent);
      } else {
        context.getCounter(PageTypes.OTHER).increment(1);
      }
    }
  }

private static class MyReducer extends Reducer<Text, Text, Text , Text> {

    public void reduce(Text key, Text value, Context context)
        throws IOException, InterruptedException {

        context.write(key, value);
    }
  }

  private static final String INPUT_OPTION = "input";
  private static final String OUTPUT_OPTION = "output";
  private static final String LANGUAGE_OPTION = "wiki_language";

  @SuppressWarnings("static-access")
  @Override
  public int run(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("XML dump file").create(INPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("en|sv|de|cs|es|zh|ar|tr").hasArg()
        .withDescription("two-letter language code").create(LANGUAGE_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String language = "en"; // Assume "en" by default.
    if (cmdline.hasOption(LANGUAGE_OPTION)) {
      language = cmdline.getOptionValue(LANGUAGE_OPTION);
      if (language.length() != 2) {
        System.err.println("Error: \"" + language + "\" unknown language!");
        return -1;
      }
    }

    String inputPath = cmdline.getOptionValue(INPUT_OPTION);
    String outputPath = cmdline.getOptionValue(OUTPUT_OPTION);

    LOG.info("Tool name: " + this.getClass().getName());
    LOG.info(" - XML dump file: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - language: " + language);

    Job job = Job.getInstance(getConf());
    job.setJarByClass(DocIdToTitle.class);
    job.setJobName(String.format("DocIdToTitle[%s: %s, %s: %s, %s: %s]", INPUT_OPTION,
        inputPath, OUTPUT_OPTION, outputPath, LANGUAGE_OPTION, language));

    job.setNumReduceTasks(1);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    if (language != null) {
      job.getConfiguration().set("wiki.language", language);
    }

    job.setInputFormatClass(WikipediaPageInputFormat.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // Delete the output directory if it exists already.
    FileSystem.get(getConf()).delete(new Path(outputPath), true);

    job.waitForCompletion(true);

    return 0;
  }

  public DocIdToTitle() {
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new DocIdToTitle(), args);
  }
}
