import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.*;
import java.util.HashMap;

public class Vector4 {
    public static class MapperClass extends Mapper<Text, Text, Text, Text> {
        public void map(Text key, Text value, Context context) throws IOException,  InterruptedException {
            //-> experience    that-comp 3092
            context.write(key, value);
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, OurVector> {
        HashMap<String,FeatureData> features;
        S3 s3;
        double [] Pf;
        double [] vec;
        String word;
        double LF;
        double l;
        int size;

        public void setup(Context context) throws IOException {
            size = 1000; //TODO
            s3 = new S3();
            features= new HashMap<>();
            Pf= new double[size];
            word="";
            LF= (double)context.getConfiguration().getLong("LF",1);
            l= 0;

            ResponseBytes<GetObjectResponse> responseBytes = s3.getObjectBytes("Step2/part-r-00000", "ass03");
            byte[] objectData = responseBytes.asByteArray();
            String path = System.getProperty("user.dir") + "/step2.txt";
            File InputFile = new File(path);
            OutputStream outputStream = new FileOutputStream(InputFile);
            outputStream.write(objectData);
            outputStream.flush();
            outputStream.close();
            try {
                BufferedReader reader = new BufferedReader(new FileReader(InputFile));
                String line = reader.readLine();
                while (line != null) {
                    // value count place
                    String [] arr = line.split("\\s+");
                    if(arr.length==3) {
                        features.putIfAbsent(arr[0],new FeatureData(Integer.parseInt(arr[2]),Long.parseLong(arr[1])));
                        Pf[Integer.parseInt(arr[2])]=Double.parseDouble(arr[1])/LF;
                    }
                    line = reader.readLine();
                }
                reader.close();
            } catch (IOException e) {
                System.out.println("Problem in function: WorkMassages");
                e.printStackTrace();
            }
        }
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            for (Text value : values) {
                //-> experience    that-comp 3092
                String[] arr = value.toString().split(" ");
                if (!key.toString().equals(word)) {
                    word = key.toString();
                    vec = new double[4 * size];
                    l = 0;
                }
                // => experience -  that-compl 3092
                l += Double.parseDouble(arr[1]);
                FeatureData f = (features.getOrDefault(arr[0], null));
                if (f != null) {
                    int location = f.getPlace() * 4;
                    vec[location] = Double.parseDouble(arr[1]); //5
                    vec[location + 1] = (double) f.getFi();
                }
            }
            double Pl = l / LF;
            for (int i = 0; i < size; i++) {
                int place = i * 4;
                double y = Pf[i] * Pl;
                double five = vec[place];  // count(l,f)
                vec[place + 1] = five / l; //6
                vec[place + 2] = Math.log10(five / y) / Math.log10(2); //7
                vec[place + 3] = (five - y) / Math.sqrt(y); //8
            }
            context.write(new Text(word), new OurVector(vec));

        }
        public void cleanup(Context context)  { }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        public int getPartition(Text key, Text value, int numPartitions) {
            return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
    }
}
