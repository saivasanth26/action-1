package studentData;

import java.io.IOException;
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

public class StudentAnalytics {

    public static class StudentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text category = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Assuming CSV format: Student Name, Institute, Program Name, Gender
            String[] fields = value.toString().split(",");

            if (fields.length >= 4) {
                String name = fields[0].trim();
                String institute = fields[1].trim();
                String program = fields[2].trim();
                String gender = fields[3].trim();

                // a. Students per Institute
                category.set("Institute: " + institute);
                context.write(category, one);

                // b. Enrolled to any program (Total count)
                category.set("Total Enrolled Students");
                context.write(category, one);

                // c. Number of boy and girl students
                category.set("Gender: " + gender);
                context.write(category, one);

                // d. Number of boy/girl students from a specific Institute (e.g., "MIT")
                // You can change "MIT" to your selected institute name
                if (institute.equalsIgnoreCase("MIT")) {
                    category.set("MIT_" + gender);
                    context.write(category, one);
                }
            }
        }
    }

    public static class StudentReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "student analytics");
        
        job.setJarByClass(StudentAnalytics.class);
        job.setMapperClass(StudentMapper.class);
        job.setReducerClass(StudentReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}