import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerOne extends Reducer<Text, IntWritable, Text, Text> {

   
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
      
        int total =0;
        for(IntWritable value:values){//emit((word,docid), sumofCounts)
            total+=value.get();
        }
        context.write(key, new Text(total+""));
        
    }
}
