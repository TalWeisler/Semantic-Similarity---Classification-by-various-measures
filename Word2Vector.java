import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.FileNotFoundException;
import java.io.IOException;

public class Word2Vector {
    public static void main(String[] args) {
        try {
            BasicConfigurator.configure();
            long LF; //L+F
            GS golden = GS.getInstance();

            //-------------------------------------------------------------
            //                 Rearrange The Result
            //-------------------------------------------------------------

            Configuration conf1 = new Configuration();
            Job job1 = Job.getInstance(conf1, "ArrangeData");
            job1.setJarByClass(ArrangeData.class);

            job1.setMapperClass(ArrangeData.MapperClass.class);
            job1.setPartitionerClass(ArrangeData.PartitionerClass.class);
            job1.setReducerClass(ArrangeData.ReducerClass.class);

            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(StringLong.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job1, new Path(args[1]));
            MultipleOutputs.addNamedOutput(job1, "Features", SequenceFileOutputFormat.class, Text.class, Text.class);
            MultipleOutputs.addNamedOutput(job1, "SortedCorpus", SequenceFileOutputFormat.class, Text.class, Text.class);
            FileOutputFormat.setOutputPath(job1, new Path("s3n://ass03/Step1"));
            job1.setInputFormatClass(SequenceFileInputFormat.class);
            job1.setOutputFormatClass(SequenceFileOutputFormat.class);
            if (job1.waitForCompletion(true)) {
                System.out.println("Finished arrangeData!!");
            }
            Counters cs = job1.getCounters();
            Counter c = cs.findCounter(ArrangeData.ReducerClass.Counter.LF);
            LF = c.getValue();
            Counter c1 = cs.findCounter(ArrangeData.ReducerClass.Counter.SC);

            if(c1.getValue()>0) {

                //-------------------------------------------------------------
                //                Take the most common Features
                //-------------------------------------------------------------

                Configuration conf2 = new Configuration();
                Job job2 = Job.getInstance(conf2, "MostCommon");
                job2.setJarByClass(MostCommon.class);
                job2.setNumReduceTasks(1);

                job2.setMapperClass(MostCommon.MapperClass.class);
                job2.setPartitionerClass(MostCommon.PartitionerClass.class);
                job2.setReducerClass(MostCommon.ReducerClass.class);

                job2.setMapOutputKeyClass(OurLong.class);
                job2.setMapOutputValueClass(Text.class);
                job2.setOutputKeyClass(Text.class);
                job2.setOutputValueClass(Text.class);

                FileInputFormat.addInputPath(job2, new Path("s3n://ass03/F"));
                FileOutputFormat.setOutputPath(job2, new Path("s3n://ass03/Step2"));
                job2.setInputFormatClass(SequenceFileInputFormat.class);
                job2.setOutputFormatClass(TextOutputFormat.class);//TODO DONT CHANGE THE FORMAT!!!
                if (job2.waitForCompletion(true)) {
                    System.out.println("Finished pick 1000 Features!!");
                }

                //-------------------------------------------------------------
                //                  Create 4 Vectors
                //-------------------------------------------------------------

                Configuration conf3 = new Configuration();
                conf3.setLong("LF", LF);
                Job job3 = Job.getInstance(conf3, "Create4Vectors");
                job3.setJarByClass(Vector4.class);

                job3.setMapperClass(Vector4.MapperClass.class);
                job3.setPartitionerClass(Vector4.PartitionerClass.class);
                job3.setReducerClass(Vector4.ReducerClass.class);

                job3.setMapOutputKeyClass(Text.class);
                job3.setMapOutputValueClass(Text.class);
                job3.setOutputKeyClass(Text.class);
                job3.setOutputValueClass(OurVector.class);

                FileInputFormat.addInputPath(job3, new Path("s3n://ass03/S"));
                FileOutputFormat.setOutputPath(job3, new Path("s3n://ass03/Step3"));
                job3.setInputFormatClass(SequenceFileInputFormat.class);
                job3.setOutputFormatClass(SequenceFileOutputFormat.class);
                if (job3.waitForCompletion(true)) {
                    System.out.println("Finished create 4 vectors!!");
                }

                //-------------------------------------------------------------
                //                Create 24 Vector for each pair
                //-------------------------------------------------------------

                Configuration conf4 = new Configuration();
                Job job4 = Job.getInstance(conf4, "FuzzyJoin");
                job4.setJarByClass(FuzzyJoin.class);
                job4.setNumReduceTasks(1);

                job4.setMapperClass(FuzzyJoin.MapperClass.class);
                job4.setPartitionerClass(FuzzyJoin.PartitionerClass.class);
                job4.setReducerClass(FuzzyJoin.ReducerClass.class);

                job4.setMapOutputKeyClass(Text.class);
                job4.setMapOutputValueClass(OurVector.class);
                job4.setOutputKeyClass(Text.class);
                job4.setOutputValueClass(Text.class);

                FileInputFormat.addInputPath(job4, new Path("s3n://ass03/Step3"));
                MultipleOutputs.addNamedOutput(job4, "WekaFile", TextOutputFormat.class, Text.class, Text.class);
                MultipleOutputs.addNamedOutput(job4, "PairFile", TextOutputFormat.class, Text.class, Text.class);
                FileOutputFormat.setOutputPath(job4, new Path(args[2]));
                job4.setInputFormatClass(SequenceFileInputFormat.class);
                job4.setOutputFormatClass(TextOutputFormat.class);
                if (job4.waitForCompletion(true)) {
                    System.out.println("Finished FuzzyJoin!!");
                }

                //-------------------------------------------------------------
                //                Create the input file to WEKA
                //-------------------------------------------------------------

                WekaFile wk = new WekaFile();
                wk.createMap();
                wk.activateWeka();
            }
            else{
                System.out.println("No word belonging to the GS was found");
           }

        } catch (InterruptedException | ClassNotFoundException | IOException e) {
            e.printStackTrace();
        }
    }
}
