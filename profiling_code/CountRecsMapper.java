import java.io.IOException;
import java.util.*; 

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CountRecsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
            
            if (key.get() == 0) {
                return;
            }
            // else{
                
            // }
            context.write(new Text("Number of Records:"), one);
            return;
        }
}
