package testWordLen;

	import java.io.IOException;
	import java.util.StringTokenizer;
	import org.apache.hadoop.conf.Configuration;
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.io.IntWritable;
	import org.apache.hadoop.io.LongWritable;
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Job;
	import org.apache.hadoop.mapreduce.Mapper;
	import org.apache.hadoop.mapreduce.Reducer;
	import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
	import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

	public class WordLen {

	    // Mapper: Outputs (Length, 1) for every word found
	    public static class LengthMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
	        private final static IntWritable one = new IntWritable(1);
	        private IntWritable wordLength = new IntWritable();

	        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	            String line = value.toString();
	            StringTokenizer tokenizer = new StringTokenizer(line);
	            
	            while (tokenizer.hasMoreTokens()) {
	                String word = tokenizer.nextToken();
	                // Get the length of the word and set it as the key
	                wordLength.set(word.length());
	                context.write(wordLength, one);
	            }
	        }
	    }

	    // Reducer: Sums the counts for each specific length
	    public static class LengthReducer extends Reducer<IntWritable, IntWritable, Text, IntWritable> {
	        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	            int sum = 0;
	            for (IntWritable val : values) {
	                sum += val.get();
	            }
	            // Formatting output as "n letter words"
	            String outputKey = key.toString() + " letter words";
	            context.write(new Text(outputKey), new IntWritable(sum));
	        }
	    }

	    public static void main(String[] args) throws Exception {
	        if (args.length != 2) {
	            System.err.println("Usage: WordLengthCount <input path> <output path>");
	            System.exit(-1);
	        }

	        Configuration conf = new Configuration();
	        Job job = Job.getInstance(conf, "word length count");
	        
	        job.setJarByClass(WordLen.class);
	        job.setMapperClass(LengthMapper.class);
	        job.setReducerClass(LengthReducer.class);

	        // Map output types
	        job.setMapOutputKeyClass(IntWritable.class);
	        job.setMapOutputValueClass(IntWritable.class);

	        // Final output types
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(IntWritable.class);

	        FileInputFormat.addInputPath(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));

	        System.exit(job.waitForCompletion(true) ? 0 : 1);
	    }
	}


