package testTemperature;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TempAnalytics {

	public static class TempMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] fields = value.toString().split(",");
			if (fields.length >= 3 && !fields[1].trim().isEmpty()) {
				String cityName = fields[2].trim();
				String temp = fields[1].trim();
				String filterCity = context.getConfiguration().get("filterCity", "");
				if (filterCity.isEmpty() || cityName.equalsIgnoreCase(filterCity)) {
					context.write(new Text(cityName), new Text(temp));
				}
			}
		}
	}

	public static class TempReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double maxTemp = Double.MIN_VALUE;
			double minTemp = Double.MAX_VALUE;
			int count = 0;
			for (Text val : values) {
				double currentTemp = Double.parseDouble(val.toString());
				if (currentTemp > maxTemp) maxTemp = currentTemp;
				if (currentTemp < minTemp) minTemp = currentTemp;
				count++;
			}
			context.write(key, new Text("Max: " + maxTemp + ", Min: " + minTemp + ", DataPoints: " + count));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if (args.length > 2) {
			conf.set("filterCity", args[2]);
		}
		Job job = Job.getInstance(conf, "temperature-analytics");
		job.setJarByClass(TempAnalytics.class);
		job.setMapperClass(TempMapper.class);
		job.setReducerClass(TempReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
}
