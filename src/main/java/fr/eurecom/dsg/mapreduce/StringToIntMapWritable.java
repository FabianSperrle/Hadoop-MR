package fr.eurecom.dsg.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/*
 * Very simple (and scholastic) implementation of a Writable associative array for String to Int 
 *
 **/
public class StringToIntMapWritable implements Writable {

    private Map<String, Integer> map;

    public StringToIntMapWritable() {
        this.map = new HashMap<>();
    }

    public StringToIntMapWritable(Map<String, Integer> map) {
        this.map = map;
    }

    public void set(Map<String, Integer> map) {
        this.map = map;
    }

    public Map<String, Integer> getMap() {
        return this.map;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.map = new HashMap<>();

        int num_words = in.readInt();
        for (int i = 0; i < num_words; i++) {
            map.put(in.readLine(), in.readInt());
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // Write map size
        out.writeInt(this.map.size());

        for (Map.Entry<String, Integer> entry : this.map.entrySet()) {
            out.writeBytes(entry.getKey());
            out.writeInt(entry.getValue());
        }
    }
}
