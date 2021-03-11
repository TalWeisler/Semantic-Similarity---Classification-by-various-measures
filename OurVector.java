import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class OurVector implements WritableComparable<OurVector> {
    private int size;
    private double[] arr;

    public OurVector(){}

    public OurVector(int l){
        size=l;
        arr= new double[l];
    }

    public OurVector(double[] a){
        arr = a;
        size= arr.length;
    }

    public double[] getArr() {
        return arr;
    }

    @Override
    public int compareTo(OurVector other) {
        return this.toString().compareTo(other.toString());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(size);
        for (int i = 0; i < arr.length; i++) {
            dataOutput.writeDouble(arr[i]);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        size= dataInput.readInt();
        arr=new double[size];
        for (int i = 0; i < size; i++) {
            arr[i] = dataInput.readDouble();
        }
    }

    @Override
    public String toString() {
        String ans="";
        for (int i = 0; i < size; i++) {
            ans=ans+" "+arr[i];
        }
        return ans;
    }

    public double[] getClone() {
        double[] copy = new double[size];
        System.arraycopy(arr, 0, copy, 0, size);
        return copy;
    }

    public double[] getArray() {
        return arr;
    }

    public void setArray(double[] array, int length) {
        arr = array;
        size = length;
    }

    public double get(int i) {
        return arr[i];
    }

    public void set(int i, double v) {
        arr[i] = v;
    }

}
