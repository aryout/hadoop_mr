import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

/**
 * Created by 97390 on 9/12/2018.
 */
public class ProvincePartitioner extends Partitioner<Text, FlowBean> {
    public static HashMap<String, Integer> proviceDict = new HashMap<String, Integer>();

    static {
        proviceDict.put("137", 0);
        proviceDict.put("133", 1);
        proviceDict.put("138", 2);
        proviceDict.put("135", 3);
    }

    @Override
    public int getPartition(Text key, FlowBean value, int numPartitions) {
        String prefix = key.toString().substring(0, 3);
        Integer provinceId = proviceDict.get(prefix);
        return provinceId == null ? 4 : provinceId;
    }
}
