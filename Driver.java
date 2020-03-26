import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.printf(
                    "Usage: %s [generic options] <input dir> <output dir>\n", getClass()
                    .getSimpleName());
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }
       
        //Get term frequency per document
        Job job = new Job(getConf());
        job.setJarByClass(Driver.class);
        job.setJobName(this.getClass().getName());

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("FL/temp1/"));

        job.setMapperClass(MapperOne.class);
        job.setReducerClass(ReducerOne.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);

        //count how many document has the term
        Job job2 = new Job(getConf());
        job2.setJarByClass(Driver.class);
        job2.setJobName(this.getClass().getName());

        FileInputFormat.setInputPaths(job2, new Path("FL/temp1/part-r-00000"));
        FileOutputFormat.setOutputPath(job2, new Path("FL/temp2"));

        job2.setMapperClass(MapperTwo.class);
        job2.setReducerClass(ReducerTwo.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.waitForCompletion(true);
         
        //compute tf*idf
        Job job3 = new Job(getConf());
        job3.setJarByClass(Driver.class);
        job3.setJobName(this.getClass().getName());

        FileInputFormat.setInputPaths(job3, new Path("FL/temp2/part-r-00000"));
        FileOutputFormat.setOutputPath(job3, new Path(args[1]));

        job3.setMapperClass(MapperThree.class);
        job3.setReducerClass(ReducerThree.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        if (job3.waitForCompletion(true)) {
            return 0;
        }
        return 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Driver(), args);
        System.exit(exitCode);
    }
}
