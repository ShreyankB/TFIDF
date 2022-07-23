import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import opennlp.tools.stemmer.PorterStemmer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class DF extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new DF(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "DF");
		job.setJarByClass(this.getClass());

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		FileSystem fs = FileSystem.get(conf);

		Path OutputPath = new Path(args[1]);
		if (fs.exists(OutputPath)) {
			fs.delete(OutputPath, true);
		}
				

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {	
		PorterStemmer stemmer = new PorterStemmer();

		@Override
		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			String line = lineText.toString();
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			Text currentWord = new Text();
			Text fN = new Text();
//			String[] tokens = line.split("[^a-zA-Z]+");			
//			String[] tokens = line.split("\\\\s*\\\\b\\\\s*");
			String[] tokens = line.split("[^\\w']+");

			for (String word : tokens) {
				if (word.isEmpty()) {
					continue;
				}
				currentWord = new Text(stemmer.stem(word.toLowerCase()));
//				currentWord = new Text(word.toLowerCase());
				fN = new Text(fileName);
				context.write(currentWord, fN);
			}			
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, IntWritable> {

		@Override
		public void reduce(Text word, Iterable<Text> counts, Context context) throws IOException, InterruptedException {
			Set<String> files = new HashSet<String>();
			for(Text fN: counts) {
				files.add(fN.toString());
			}
			context.write(word, new IntWritable(files.size()));
		}

	}
}
