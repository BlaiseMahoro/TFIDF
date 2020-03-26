import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//import sun.launcher.resources.launcher;

public class ReducerTwo extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        ArrayList<Text> myValues = new ArrayList<Text>();//To solve the problem of iterating twice on the same iterable
        int terms =0;
        String word = key.toString();
        for(Text v:values){//this loop counts how many docs cntain the word.
            String [] arr = v.toString().trim().split(",");
            int count =Integer.parseInt(arr[2].trim());
            terms +=count;
            myValues.add(v);
        }    
        for(Text v:myValues){//emit((word, docid), (tf,terms))
            String [] arr = v.toString().trim().split(",");
            String docId = arr[0].trim();
            String tf = arr[1].trim();
            context.write(new Text(word+","+docId), new Text(tf+","+terms));
        }
       
       
    }
}
