import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperOne extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static String [] WORDS = {"i", "me","my","myself","we","our","ours",
    "ourselves","you","your","yours","yourself","yourselves","he","him","his","himself",
    "she","her","hers","herself", "it","its","itself", "they","them","their","theirs",
    "themselves","what","which","who","whom","this","that","these","those","am",
    "is","are","was","were","be","been","being","have","has","had","having","do",
    "does","did","doing","a","an","the","and","but","if","or","because","as","until",
    "while","of","at","by","for","with","about","against","between","into","through","during",
    "before","after","above","below","to","from","up","down","in","out","on","off","over","under",
    "again","further","then","once","here","there","when","where","why","how","all","any","both",
    "each","few","more","most","other","some","such", "no","nor","not","only","own","same","so",
    "than","too","very","s","t","can","will","just","don","should","now"};
    private static List<String> myList=Arrays.asList(WORDS);
    private String docId = "";
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String s = value.toString();
        
        if(s.contains("==========================")){
            //get docId
            docId= s.replace("=", "").trim();
        }
        else{
            //emit the docId with the word as key, and 1 as value.
            String[] words = s.split("[^a-zA-Z]"); 
            for(String word :words){
                if(!myList.contains(word.toLowerCase())&&!word.trim().isEmpty()){
                    //makes sure the word is not in the stopword.txt or the word is a white space character.
                String key_string = docId+","+word;
                context.write(new Text(key_string), new IntWritable(1));
                }
            }
           
        } 
       

    }
    

}