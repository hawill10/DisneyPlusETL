import java.io.IOException;
import java.util.*; 

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieFilterMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    private static final IntWritable one = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
            String line = value.toString();
            List<String> info = Arrays.asList(line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"));

            if (key.get() == 0) {

                context.write(key, new Text("imdb_id,title,age"));
                return;
            }
            else{
                if (info.size() == 19 && info.get(3).equals("movie") ) {

                    if(info.get(6).split(" ").length < 3) {
                        return;
                    }
                    String released_year = info.get(6).split(" ")[2];

                    if (released_year.compareTo("2019") <= 0) {
                        List<Integer> filterIndices = Arrays.asList(0, 1, 4);
                        List<String> filteredList = new ArrayList<String>();
                        for(int i=0; i < info.size(); i++){
                            if(filterIndices.contains(i)){
                                if (info.get(i) == "") {
                                    return;
                                }
                                filteredList.add(info.get(i));
                            }
                        }
                        String result = String.join(",", filteredList);
                        context.write(key, new Text(result));
                    }
                }

                return;
            }
        }
}
