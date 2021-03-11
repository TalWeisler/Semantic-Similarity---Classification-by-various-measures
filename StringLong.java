import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StringLong implements WritableComparable<StringLong> {
    private String word;
    private Long count;

    public StringLong(){}
    public StringLong(String w,Long c){
        word= w;
        count= c;
    }

    @Override
    public int compareTo(StringLong other) {
        return this.toString().compareTo(other.toString());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(word);
        dataOutput.writeLong(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        word= dataInput.readUTF();
        count= dataInput.readLong();
    }

    public long getCount (){
        return count;
    }

    public String getWord (){
        return word;
    }

    @Override
    public String toString() {
        return word+" "+count.toString();
    }
}
