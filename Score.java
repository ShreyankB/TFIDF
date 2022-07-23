
import opennlp.tools.stemmer.PorterStemmer;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Score extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Score(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		FileSystem fs = FileSystem.get(getConf());

		Path OutputPath = new Path(args[1]);
		if (fs.exists(OutputPath)) {
			fs.delete(OutputPath, true);
		}

	
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Score");

		if (fs.exists(OutputPath)) {
			fs.delete(OutputPath, true);
		}

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.addCacheFile(new Path(args[2]).toUri());

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		PorterStemmer stemmer = new PorterStemmer();

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			String line = lineText.toString();
//			String[] tokens = line.split("[^a-zA-Z]+");			
//			String[] tokens = line.split("\\\\s*\\\\b\\\\s*");
			String[] tokens = line.split("[^\\w']+");			
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

			Text currentWord = new Text();
			for (String word : tokens) {
				if (word.isEmpty()) {
					continue;
				}

				currentWord = new Text(stemmer.stem(word.toLowerCase()) + "->" + fileName);
				context.write(currentWord, one);
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {
		HashMap<String, Integer> DF = null;
		
		public void setup(Context context) throws IOException, InterruptedException {
			DF = new HashMap<String, Integer>();
			Configuration conf = context.getConfiguration();
			URI[] cacheFiles = Job.getInstance(conf).getCacheFiles();

			for (URI cacheFile : cacheFiles) {
				Path filePath = new Path(cacheFile.getPath());
				String fileName = filePath.getName().toString();

				try {
					BufferedReader reader = new BufferedReader(new FileReader(fileName));
					String line = null;
					while ((line = reader.readLine()) != null) {
						String[] words = line.split("[^\\w']+");
						DF.put(words[0], Integer.parseInt(words[1]));
					}
					reader.close();
				} catch (IOException ioe) {
					System.err.println(
							"Caught exception while parsing the cached file '" + StringUtils.stringifyException(ioe));
				}

			}
		}

		@Override
		public void reduce(Text word, Iterable<IntWritable> counts, Context context)
				throws IOException, InterruptedException {
			int TF = 0;
			
			for (IntWritable count : counts) {
				TF += count.get();
			}
			
			int df_score = 0;
			String[] to_search = word.toString().split("->");

			if(DF.containsKey(to_search[0]))
				df_score = DF.get(to_search[0]);
						
			double score = TF * Math.log10(10000 / (df_score+1));
			word = new Text(to_search[1] + "	" + to_search[0] + "	");
			context.write(word, new DoubleWritable(score));
		}
	}
}
