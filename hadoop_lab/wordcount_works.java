package org.elliot.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;

import org.apache.commons.logging.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    public static final Log log = LogFactory.getLog(TokenizerMapper.class);

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
	String currString = itr.nextToken();
	if (Arrays.asList(stopWords).contains(currString)) {
       // if (currString.equals("a")) {
		//log.info("GOTCHA");
	}
	else {
		context.write(new Text(currString), new IntWritable(1));
		System.out.println(currString + " ");
	}
      }
    }
  }

  public static class IntTextCombo implements Comparable<IntTextCombo> {
	public IntWritable intval;
	public Text str;

	@Override
	public int compareTo( final IntTextCombo other) {
		return Integer.compare(intval.get(), other.intval.get());
	}

	@Override
	public String toString() {
		return str.toString() + " " + intval.toString();
	}
  }

  public static class IntSumReducer
	  extends Reducer<Text,IntWritable,Text,IntWritable> {
		  private IntWritable result = new IntWritable();
		  private boolean donezo = false;

		  public static ArrayList<IntTextCombo> topWords = new ArrayList<IntTextCombo>();

		  public void reduce(Text key, Iterable<IntWritable> values,
				  Context context
				  ) throws IOException, InterruptedException {
			  int sum = 0;
			  for (IntWritable val : values) {
				  sum += val.get();
			  }
			  //      result.set(sum);
			  //      context.write(key, result);
			  IntTextCombo combo = new IntTextCombo();
			  combo.intval = new IntWritable(sum);
			  combo.str = new Text(key);
			  topWords.add(combo);
			  System.out.println("Hey: " + topWords.size() + " " + combo.str.toString() + ", " + combo.intval);
		  }

		  @Override
			  protected void cleanup(Context context) throws IOException, InterruptedException {
				  Collections.sort(topWords);
				  Collections.reverse(topWords);
				  System.out.println("IT IS THIS BIG: " + topWords.size());

				  if (donezo) {
					  return;
				  }

				  for (int ndx = 0; ndx < 10; ndx++) {
					  if (ndx >= topWords.size()) {
						  break;
					  }
					System.out.println("Writing this to the end: " + topWords.get(ndx).str + " " + topWords.get(ndx).intval);
					  context.write(topWords.get(ndx).str, topWords.get(ndx).intval);

					  donezo = true;
				  }
			  }
  }

  public static void main(String[] args) throws Exception {
    System.out.println("Oh boy here we go!");
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

 public static String[] stopWords = {
"a",
"about",
"above",
"across",
"after",
"afterwards",
"again",
"against",
"all",
"almost",
"alone",
"along",
"already",
"also",
"although",
"always",
"am",
"among",
"amongst",
"amoungst",
"amount",
"an",
"and",
"another",
"any",
"anyhow",
"anyone",
"anything",
"anyway",
"anywhere",
"are",
"around",
"as",
"at",
"back",
"be",
"became",
"because",
"become",
"becomes",
"becoming",
"been",
"before",
"beforehand",
"behind",
"being",
"below",
"beside",
"besides",
"between",
"beyond",
"bill",
"both",
"bottom",
"but",
"by",
"call",
"can",
"cannot",
"cant",
"co",
"computer",
"con",
"could",
"couldnt",
"cry",
"de",
"describe",
"detail",
"do",
"done",
"down",
"due",
"during",
"each",
"eg",
"eight",
"either",
"eleven",
"else",
"elsewhere",
"empty",
"enough",
"etc",
"even",
"ever",
"every",
"everyone",
"everything",
"everywhere",
"except",
"few",
"fifteen",
"fify",
"fill",
"find",
"fire",
"first",
"five",
"for",
"former",
"formerly",
"forty",
"found",
"four",
"from",
"front",
"full",
"further",
"get",
"give",
"go",
"had",
"has",
"hasnt",
"have",
"he",
"hence",
"her",
"here",
"hereafter",
"hereby",
"herein",
"hereupon",
"hers",
"herse",
"him",
"himse",
"his",
"how",
"however",
"hundred",
"i",
"ie",
"if",
"in",
"inc",
"indeed",
"interest",
"into",
"is",
"it",
"its",
"itse",
"keep",
"last",
"latter",
"latterly",
"least",
"less",
"ltd",
"made",
"many",
"may",
"me",
"meanwhile",
"might",
"mill",
"mine",
"more",
"moreover",
"most",
"mostly",
"move",
"much",
"must",
"my",
"myse",
"name",
"namely",
"neither",
"never",
"nevertheless",
"next",
"nine",
"no",
"nobody",
"none",
"noone",
"nor",
"not",
"nothing",
"now",
"nowhere",
"of",
"off",
"often",
"on",
"once",
"one",
"only",
"onto",
"or",
"other",
"others",
"otherwise",
"our",
"ours",
"ourselves",
"out",
"over",
"own",
"part",
"per",
"perhaps",
"please",
"put",
"rather",
"re",
"same",
"see",
"seem",
"seemed",
"seeming",
"seems",
"serious",
"several",
"she",
"should",
"show",
"side",
"since",
"sincere",
"six",
"sixty",
"so",
"some",
"somehow",
"someone",
"something",
"sometime",
"sometimes",
"somewhere",
"still",
"such",
"system",
"take",
"ten",
"than",
"that",
"the",
"their",
"them",
"themselves",
"then",
"thence",
"there",
"thereafter",
"thereby",
"therefore",
"therein",
"thereupon",
"these",
"they",
"thick",
"thin",
"third",
"this",
"those",
"though",
"three",
"through",
"throughout",
"thru",
"thus",
"to",
"together",
"too",
"top",
"toward",
"towards",
"twelve",
"twenty",
"two",
"un",
"under",
"until",
"up",
"upon",
"us",
"very",
"via",
"was",
"we",
"well",
"were",
"what",
"whatever",
"when",
"whence",
"whenever",
"where",
"whereafter",
"whereas",
"whereby",
"wherein",
"whereupon",
"wherever",
"whether",
"which",
"while",
"whither",
"who",
"whoever",
"whole",
"whom",
"whose",
"why",
"will",
"with",
"within",
"without",
"would",
"yet",
"you",
"your",
"yours",
"yourself",
"yourselves"
  };
}
