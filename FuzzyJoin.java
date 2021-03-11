import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class FuzzyJoin {
    public static class MapperClass extends Mapper<Text, OurVector, Text, OurVector> {
        private GS golden;

        public void setup(Context context){
            golden = GS.getInstance();

        }
        public void map(Text key, OurVector vec, Context context) throws IOException,  InterruptedException {
            Set<String> set = golden.getSetFromPairs(key.toString());
            if (set != null) {
                for (String value : set) {
                    if (key.toString().compareTo(value) < 0) {
                        context.write(new Text(key.toString() + " " + value), vec);
                    } else if (key.toString().compareTo(value) > 0) {
                        context.write(new Text(value + " " + key.toString()), vec);
                    }

                }
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, OurVector, Text, Text> {
        String pair;
        double [] first;
        int counter;
        private GS golden;
        private MultipleOutputs m;

        public void setup(Context context) throws IOException, InterruptedException {
            pair = "";
            golden = GS.getInstance();
            m= new MultipleOutputs(context);
            counter = 0;

            String start_weka_file = "@relation wordSimilarity\n\n" +
                    "@attribute fiveAndNine real\n" +
                    "@attribute fiveAndTen real\n" +
                    "@attribute fiveAndEleven real\n" +
                    "@attribute fiveAndThirteen real\n" +
                    "@attribute fiveAndFifteen real\n" +
                    "@attribute fiveAndSeventeen real\n" +
                    "@attribute sixAndNine real\n" +
                    "@attribute sixAndTen real\n" +
                    "@attribute sixAndEleven real\n" +
                    "@attribute sixAndThirteen real\n" +
                    "@attribute sixAndFifteen real\n" +
                    "@attribute sixAndSeventeen real\n" +
                    "@attribute sevenAndNine real\n" +
                    "@attribute sevenAndTen real\n" +
                    "@attribute sevenAndEleven real\n" +
                    "@attribute sevenAndThirteen real\n" +
                    "@attribute sevenAndFifteen real\n" +
                    "@attribute sevenAndSeventeen real\n" +
                    "@attribute eightAndNine real\n" +
                    "@attribute eightAndTen real\n" +
                    "@attribute eightAndEleven real\n" +
                    "@attribute eightAndThirteen real\n" +
                    "@attribute eightAndFifteen real\n" +
                    "@attribute eightAndSeventeen real\n" +
                    "@attribute class {yes, no}\n\n" +
                    "@data\n";
            m.write("WekaFile", new Text(start_weka_file), new Text());
        }

        public void reduce(Text key, Iterable<OurVector> values, Context context) throws IOException,  InterruptedException {
            for (OurVector value : values) {
                if (!key.toString().equals(pair)){
                    pair = key.toString();
                    first = value.getArr();
                }
                else {
                    int size= 1000; //TODO
                    double [] second = value.getArr();
                    double [] result = new double[24];
                    double [][] li = new double[2][4];
                    double [][] minmax = new double[2][4]; //0 -min, 1-max
                    for (int i = 0; i < size; i++) {
                        li[0][0] += Math.pow(first[i*4], 2);
                        li[0][1] += Math.pow(first[i*4 + 1], 2);
                        li[0][2] += Math.pow(first[i*4 + 2], 2);
                        li[0][3] += Math.pow(first[i*4 + 3], 2);
                        li[1][0] += Math.pow(second[i*4], 2);
                        li[1][1] += Math.pow(second[i*4 + 1], 2);
                        li[1][2] += Math.pow(second[i*4 + 2], 2);
                        li[1][3] += Math.pow(second[i*4 + 3], 2);
                        minmax [0][0] += Math.min(first[i*4] , second[i*4]);
                        minmax [0][1] += Math.min(first[i*4 + 1] , second[i*4 + 1]);
                        minmax [0][2] += Math.min(first[i*4 + 2] , second[i*4 + 2]);
                        minmax [0][3] += Math.min(first[i*4 + 3] , second[i*4 + 3]);
                        minmax [1][0] += Math.max(first[i*4] , second[i*4]);
                        minmax [1][1] += Math.max(first[i*4 + 1] , second[i*4 + 1]);
                        minmax [1][2] += Math.max(first[i*4 + 2] , second[i*4 + 2]);
                        minmax [1][3] += Math.max(first[i*4 + 3] , second[i*4 + 3]);
                        result[0] += Math.abs(first[i*4] - second[i*4]); //5 , 9
                        result[1] += Math.pow((first[i*4] - second[i*4]),2); //5 , 10
                        result[2] += (first[i*4] * second[i*4]); //5, 11
                        result[4] += (first[i*4] + second[i*4]); //5, 15 (den)
                        result[5] += (first[i*4] * Math.log(2 * first[i*4]/(first[i*4] + second[i*4]))); //5, 17
                        result[5] += (second[i*4] * Math.log(2 * second[i*4]/(first[i*4] + second[i*4]))); //5, 17

                        result[6] += Math.abs(first[i*4 + 1] - second[i*4 + 1]); //6 , 9
                        result[7] += Math.pow((first[i*4 + 1] - second[i*4 + 1]),2); //6 , 10
                        result[8] += (first[i*4 + 1] * second[i*4 + 1]); //6, 11
                        result[10] += (first[i*4 + 1] + second[i*4 + 1]); //6, 15 (den)
                        result[11] += (first[i*4 + 1] * Math.log(2 * first[i*4 + 1]/(first[i*4 + 1] + second[i*4 + 1]))); //6, 17
                        result[11] += (second[i*4 + 1] * Math.log(2 * second[i*4 + 1]/(first[i*4 + 1] + second[i*4 + 1]))); //6, 17

                        result[12] += Math.abs(first[i*4 + 2] - second[i*4 + 2]); //7 , 9
                        result[13] += Math.pow((first[i*4 + 2] - second[i*4 + 2]),2); //7 , 10
                        result[14] += (first[i*4 + 2] * second[i*4 + 2]); //7, 11
                        result[16] += (first[i*4 + 2] + second[i*4 + 2]); //7, 15 (den)
                        result[17] += (first[i*4 + 2] * Math.log(2 * first[i*4 + 2]/(first[i*4 + 2] + second[i*4 + 2]))); //7, 17
                        result[17] += (second[i*4 + 2] * Math.log(2 * second[i*4 + 2]/(first[i*4 + 2] + second[i*4 + 2]))); //7, 17

                        result[18] += Math.abs(first[i*4 + 3] - second[i*4 + 3]); //8 , 9
                        result[19] += Math.pow((first[i*4 + 3] - second[i*4 + 3]),2); //8 , 10
                        result[20] += (first[i*4 + 3] * second[i*4 + 3]); //8, 11
                        result[22] += (first[i*4 + 3] + second[i*4 + 3]); //8, 15 (den)
                        result[23] += (first[i*4 + 3] * Math.log(2 * first[i*4 + 3]/(first[i*4 + 3] + second[i*4 + 3]))); //8, 17
                        result[23] += (second[i*4 + 3] * Math.log(2 * second[i*4 + 3]/(first[i*4 + 3] + second[i*4 + 3]))); //8, 17
                    }
                    for (int i = 0; i < 4; i++) {
                        result[i*6 + 1] = Math.sqrt(result [i*6 + 1]); //10
                        result[i*6 + 2] = (result[i*6 + 2])/(Math.sqrt(li[0][i]) * Math.sqrt(li[1][i])); //11
                        result[i*6 + 3] = minmax[0][i]/minmax[1][i]; //13
                        result[i*6 + 4] = 2*minmax[0][i]/result[i*6 +4]; //15
                    }

                    String ans="";
                    for (int i = 0; i < result.length; i++) {
                        String temp = ""+result[i];
                        if (temp.equals("NaN"))
                            ans = ans + "?,";
                        else
                            ans = ans + result[i] + ",";
                    }

                    String [] arr = pair.split(" ");
                    String val= golden.getValue(arr[0],arr[1]);
                    if(val == null)
                        val="";
                    else {
                        ans= ans.concat(val);
                        val = " " + val;
                    }
                    //m.write("SortedCorpus",new Text(feature),new Text("1 "+value.toString()));
                    m.write("WekaFile", new Text(ans), new Text());
                    m.write("PairFile", new Text(String.valueOf(counter)), new Text(pair + val));
                    counter++;
                }
            }
        }
        public void cleanup(Context context)  {
            try {
                m.close();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, OurVector> {
        public int getPartition(Text key, OurVector value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
}
