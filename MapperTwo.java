import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperTwo extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //Expects value: word, docId   tf.
        //needs to split this to get individual fields.
        String s = value.toString();
        String [] arr=s.trim().split("\t");
        String wrd_docId = arr[0];//(word,docId)
        String [] arr1 =wrd_docId.split(","); //
        String docId = arr1[0];
        String word =arr1[1];
        String tf= arr[1];//(tf)
       
        context.write(new Text(word), new Text(docId+","+tf+","+1)) ;//emit key: airport code and 
        
       

    }

}