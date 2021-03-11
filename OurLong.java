import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OurLong implements WritableComparable<OurLong> {
    private Long num;

    public OurLong(){}
    public OurLong(Long n){
        num = n;
    }

    @Override
    public int compareTo(OurLong other) {
        if (other.getNum() == num)
            return 0;
        else if (other.getNum() > num)
            return 1;
        else
            return -1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(num);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        num= dataInput.readLong();
    }

    public Long getNum() {
        return num;
    }

    @Override
    public String toString() {
        return num.toString();
    }
}
