import java.io.IOException;
import java.util.*; 

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    private static final IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
            String line = value.toString();
            List<String> info = Arrays.asList(line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"));

            if (key.get() == 0) {

                context.write(key, new Text("Title"));
                return;
            }
            else{
                if (!info.get(0).equals("Title") && line.length() != 0) {
                    String title = info.get(0);
                    String regex = "\\[.*\\]";

                    title = title.replaceAll(regex, "").trim();
                    
                    context.write(key, new Text(title));

                }

                return;
            }
        }
}
