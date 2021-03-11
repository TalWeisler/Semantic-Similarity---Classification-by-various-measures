import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

public class ArrangeData {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, StringLong> {
        public void map(LongWritable lineId, Text line, Context context) throws IOException,  InterruptedException {
            //we save 16 places
            // example: experience      that/IN/compl/3 patients/NNS/nsubj/3 experience/VB/ccomp/0      3092
            // [experience, that/IN/compl/3, patients/NNS/nsubj/3, experience/VB/ccomp/0, 3092]
            String [] arr1 = line.toString().split("\\s+");
            if (arr1.length >= 5) {
                // [[that,IN,compl,3],[patients,NNS,nsubj,3],[experience,VB,ccomp,0]]
                int num_words = 1;
                boolean found = false;
                while (num_words < arr1.length && !found){
                    try {
                        Long n = Long.parseLong(arr1[num_words]);
                        found = true;
                        num_words --;
                    }
                    catch (NumberFormatException numberFormatException){
                        num_words ++;
                    }
                }
                if (num_words > 0 && found) {
                    String[][] arr2 = new String[num_words][];
                    Stemmer s = new Stemmer();
                    for (int i = 0; i < num_words; i++) {
                        arr2[i] = arr1[i+1].split("/");
                        arr2[i][0]=s.run(arr2[i][0]);
                    }
                    for (int i = 0; i < num_words; i++) {
                        int j = -1;
                        if (arr2[i].length == 4){
                            try {
                                j = Integer.parseInt(arr2[i][3]) - 1;
                            } catch (NumberFormatException numberFormatException){
                                System.err.println("there was a problem with line: " + line);
                            }
                        }
                        else if (arr2[i].length == 5){
                            try {
                                j = Integer.parseInt(arr2[i][4]) - 1;
                            } catch (NumberFormatException numberFormatException){
                                System.err.println("there was a problem with line: " + line);
                            }
                        }
                        if (j != -1) {
                            // <<that-compl> , <experience, 3092>>
                            context.write(new Text(arr2[i][0] + "-" + arr2[i][2]), new StringLong(arr2[j][0], Long.parseLong(arr1[num_words + 1])));
                        }
                    }
                }
            }
            else {
                System.out.println("there is a line that it's split length does not equal 5 : "+ line.toString());
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, StringLong,Text,Text> {
        enum Counter{
            LF, SC
        }
        private MultipleOutputs m;
        private Long count;
        private String feature;
        private GS golden;
        String OutputPath1;
        String OutputPath2;

        public void setup(Context context){
            count = 0L;
            feature = "";
            m = new MultipleOutputs(context);
            golden = GS.getInstance();
            OutputPath1="s3n://ass03/F/Features";
            OutputPath2="s3n://ass03/S/SortedCorpus";
        }
        public void reduce(Text key, Iterable<StringLong> values, Context context) throws IOException,  InterruptedException {
            for (StringLong value : values) {
                // <<that-compl> , <experience, 3092>>
                if(!key.toString().equals(feature)){
                    feature = key.toString();
                    count = 0L;
                }
                count += value.getCount();
                context.getCounter(ArrangeData.ReducerClass.Counter.LF).increment(value.getCount());
                if (golden.ContainInWords(value.getWord())){
                    context.getCounter(ArrangeData.ReducerClass.Counter.SC).increment(1);
                    //-> experience    that-comp 3092
                    m.write("SortedCorpus",new Text(value.getWord()),new Text(feature+" "+value.getCount()),OutputPath2);
                }
            }
            m.write("Features",new Text(feature),new Text(count.toString()),OutputPath1);
        }
        public void cleanup(Context context)  {
            try {
                m.close();
                }
            catch (IOException | InterruptedException e) {
                System.out.println("Problem in the reduce of arrangeData");
                e.printStackTrace();
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, StringLong> {
        public int getPartition(Text key, StringLong value, int numPartitions) {
           return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
}
