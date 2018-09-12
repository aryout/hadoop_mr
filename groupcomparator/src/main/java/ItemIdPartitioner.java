import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by 97390 on 9/12/2018.
 */
public class ItemIdPartitioner extends Partitioner<OrderBean, NullWritable> {
    @Override public int getPartition(OrderBean bean, NullWritable value, int numReduceTasks) {
        return (bean.getItemid().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}


