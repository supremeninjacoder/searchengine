
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.lang.*;

import org.apache.commons.lang.StringUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ToolRunner;

class PagePair implements Comparable<PagePair> {
	Integer first;
	float second;
	public PagePair(Integer a,float b){
		first = a;
		second = b;	
	}
	@Override
	public int compareTo(PagePair o) {
	return second > o.second ? -1 : second < o.second ? 1 : 0;
	}
}

class TermPair implements Comparable<TermPair> {
	String first;
	int second;
	public TermPair(String a,int b){
		first = a;
		second = b;	
	}
	@Override
	public int compareTo(TermPair o) {
	return second < o.second ? -1 : second > o.second ? 1 : 0;
	}
}
public class SearchTerms {

  public static void lookupTerms(String[] terms, MapFile.Reader reader, MapFile.Reader reader1, MapFile.Reader reader2, FileSystem fs) throws IOException {
	
	ArrayList<Integer> list = new ArrayList<Integer>();
	List<TermPair> searchTerms = new ArrayList<TermPair>();

	for(int i=3;i<terms.length;i++){
		Text key = new Text();
		ArrayListWritable<IntWritable> value = new ArrayListWritable<IntWritable>();
		key.set(terms[i].toLowerCase());
		Writable w = reader.get(key, value);
		if (w == null) {
			searchTerms.add(new TermPair(terms[i].toLowerCase(),0));
		}
		else {
			searchTerms.add(new TermPair(terms[i].toLowerCase(),value.getSize()));
		}	
	}
	Collections.sort(searchTerms);
	int firstTerm = 1;
	for(TermPair entry : searchTerms){
		Text key = new Text();
		ArrayListWritable<IntWritable> value = new ArrayListWritable<IntWritable>();
		key.set(entry.first);
		Writable w = reader.get(key, value);
		if (w==null){
			System.out.println("No Search Results Found");
			return;		
		}
		ArrayList<Integer> L = value.getArrayList();
		if(firstTerm==1)
			list = L;
		list.retainAll(L);
		firstTerm = 0;
	}

	List<PagePair> docNoWithRankings = new ArrayList<PagePair>();
	for(int docNo : list){
		FloatWritable value1 = new FloatWritable();
		Writable w1 = reader1.get(new Text(Integer.toString(docNo)), value1);
		docNoWithRankings.add(new PagePair(docNo,value1.get()));
	}
	Collections.sort(docNoWithRankings);
	int count = 1;
	System.out.println("Search Results :");
	for(PagePair entry : docNoWithRankings){
		Text value2 = new Text();
		Writable w = reader2.get(new Text(Integer.toString(entry.first)),value2);
		System.out.println(count+". "+value2.toString() + " " + entry.second);
		count = count + 1;
		if(count == 11)
			break;
	}
  }

  public static void runJob(String[] input) throws Exception {

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    MapFile.Reader reader = new MapFile.Reader(new Path(input[0] + "/part-r-00000"), conf);
    MapFile.Reader reader1 = new MapFile.Reader(new Path(input[1] + "/part-r-00000"), conf);
    MapFile.Reader reader2 = new MapFile.Reader(new Path(input[2] + "/part-r-00000"), conf);
    lookupTerms(input, reader, reader1, reader2, fs);

  }

  public static void main(String[] args) throws Exception {
       runJob(Arrays.copyOfRange(args, 0, args.length));
  }

}


