import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieFilter {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MovieFilter <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job();
        job.setJarByClass(MovieFilter.class);
        job.setJobName("MovieFilter");
        job.setNumReduceTasks(1);   

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MovieFilterMapper.class);
        job.setReducerClass(MovieFilterReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}