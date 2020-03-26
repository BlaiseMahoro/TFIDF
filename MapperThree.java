import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperThree extends Mapper<LongWritable, Text, Text, Text> {

    private static final int N = 10788;
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
                
        //expected value: word, docId  tf,n
        //split this and get individual values.
        String s = value.toString();
        String [] arr=s.trim().split("\t"); 
        String []tf_n = arr[1].trim().split(",");
        int tf = Integer.parseInt(tf_n[0]);
        int n = Integer.parseInt(tf_n[1].trim());
        double idf = Math.log(N/n);
        double tfidf= (double)tf*idf ;
        //emit((word, docId), (tf,n))
        context.write(new Text(arr[0]), new Text(tfidf+"") );
    }

}