import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MostCommon {
    public static class MapperClass extends Mapper<Text, Text, OurLong, Text> {
        public void map(Text key, Text value, Context context) throws IOException,  InterruptedException {
            // that-compl 10
            context.write(new OurLong(Long.parseLong(value.toString())), key);
        }
    }

    public static class ReducerClass extends Reducer<OurLong, Text, Text,Text> {
        private int count100;
        private int count1000;
        private int size;

        public void setup(Context context){
            count100 = 100;
            count1000 = 0;
            size = 1000;
        }
        public void reduce(OurLong key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            if (count1000 < size) {
                for (Text value : values) {
                    if (count100 > 0) {
                        count100--;
                    } else if (count1000 < size) {
                        context.write(value, new Text(key.toString() + " " + count1000));
                        count1000++;
                    }
                }
            }
        }
        public void cleanup(Context context)  { }
    }

    public static class PartitionerClass extends Partitioner<OurLong, Text> {
        public int getPartition(OurLong key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
}
