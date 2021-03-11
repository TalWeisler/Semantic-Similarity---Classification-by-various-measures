import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import weka.classifiers.Evaluation;
import weka.classifiers.bayes.NaiveBayes;
import weka.core.Instance;
import weka.core.Instances;
import java.io.*;
import java.util.HashMap;
import java.util.Random;

public class WekaFile {

    private S3 s3;
    private HashMap<Integer, String> map;

    public WekaFile() throws FileNotFoundException {
        s3 = new S3();
        map = new HashMap<>();
    }

    public void createMap () throws IOException {
        ResponseBytes<GetObjectResponse> responseBytes = s3.getObjectBytes("Step4/PairFile-r-00000", "ass03");
        byte[] objectData = responseBytes.asByteArray();
        String path = System.getProperty("user.dir") + "/step4.txt";
        File InputFile = new File(path);
        OutputStream outputStream = new FileOutputStream(InputFile);
        outputStream.write(objectData);
        outputStream.flush();
        outputStream.close();

        try {
            BufferedReader reader = new BufferedReader(new FileReader(InputFile));
            String line = reader.readLine();
            while (line != null) {
                //line = 0	abattoir jet no
                String [] arr = line.toString().split("\\s+");
                map.putIfAbsent(Integer.parseInt(arr[0]), arr[1]+" "+arr[2]+" "+arr[3]);
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            System.out.println("Problem in function: WorkMassages");
            e.printStackTrace();
        }
    }

    public void activateWeka() throws IOException {
        //downloading the file with all the 24 length vectors from S3
        ResponseBytes<GetObjectResponse> responseBytes = s3.getObjectBytes("Step4/WekaFile-r-00000", "ass03");
        byte[] objectData = responseBytes.asByteArray();
        String path = System.getProperty("user.dir") + "/step4.txt";
        File InputFile = new File(path);
        OutputStream outputStream = new FileOutputStream(InputFile);
        outputStream.write(objectData);
        outputStream.flush();
        outputStream.close();

        BufferedReader bufferedReader = new BufferedReader(new FileReader(path));

        Instances trainSet = new Instances(bufferedReader);
        trainSet.setClassIndex(trainSet.numAttributes() - 1); //set the class attribute to our file to be the last one, {yes, no}

        bufferedReader.close();

        NaiveBayes naiveBayes = new NaiveBayes();
        try {
            naiveBayes.buildClassifier(trainSet);
            Evaluation eval = new Evaluation(trainSet);
            eval.crossValidateModel(naiveBayes, trainSet, 10, new Random(1)); //making 10 crossValidation
            File result = new File(System.getProperty("user.dir")+"/Final.txt");
            FileOutputStream fos = new FileOutputStream(result, true);
            fos.write(("\n***The results: ***\n").getBytes());
            fos.write(("Correctly Classified Instances: ").getBytes());
            fos.write((eval.pctCorrect()+"%\n").getBytes());
            fos.write(("Incorrectly Classified Instances ").getBytes());
            fos.write((eval.pctIncorrect()+"%\n").getBytes());
            String s= "F1: " + eval.fMeasure(1) + " \nPrecision: " + eval.precision(1) + " \nRecall: " + eval.recall(1);
            fos.write(s.getBytes());
            fos.close();
            s3.PutObject(System.getProperty("user.dir")+"/Final.txt","ass03","Final");

            //finding out the true-positive, false-positive, true-negative, and false-negative classes
            File analysis = new File(System.getProperty("user.dir")+"/Analysis.txt");
            FileOutputStream fos2 = new FileOutputStream(analysis, true);
            for (int i = 0; i < trainSet.numInstances(); i++) {
                String pair = map.get(i);
                String [] pArr = pair.split("\\s+");
                Instance instance = trainSet.instance(i);
                double pred = naiveBayes.classifyInstance(instance);
                String predClass = trainSet.classAttribute().value((int) pred);
                if (predClass.equals("yes") && pArr[2].equals("yes")){
                    fos2.write(("The pair: " + pArr[0]+" "+pArr[1] + ", "+ "the predicted Class is: "+ predClass +", the index is: " +i+" *** true-positive ***\n").getBytes());
                }
                else if (predClass.equals("yes") && pArr[2].equals("no")){
                    fos2.write(("The pair: " + pArr[0]+" "+pArr[1] + ", "+ "the predicted Class is: "+ predClass +", the index is: " +i+" *** false-positive ***\n").getBytes());
                }
                else if (predClass.equals("no") && pArr[2].equals("yes")){
                    fos2.write(("The pair: " + pArr[0]+" "+pArr[1] + ", "+ "the predicted Class is: "+ predClass +", the index is: " +i+" *** false-negative ***\n").getBytes());
                }
                else if (predClass.equals("no") && pArr[2].equals("no")){
                    fos2.write(("The pair: " + pArr[0]+" "+pArr[1] + ", "+ "the predicted Class is: "+ predClass +", the index is: " +i+" *** true-negative ***\n").getBytes());
                }
            }
            fos2.close();
            s3.PutObject(System.getProperty("user.dir")+"/Analysis.txt","ass03","Analysis");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
