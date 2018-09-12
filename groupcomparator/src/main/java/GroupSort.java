import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by 97390 on 9/12/2018.
 */
public class GroupSort {
    static class SortMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
        OrderBean bean = new OrderBean();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            bean.set(new Text(fields[0]), new DoubleWritable(Double.parseDouble(fields[2])));
            context.write(bean, NullWritable.get());
        }
    }

    static class SortReducer extends Reducer<OrderBean, NullWritable,OrderBean,NullWritable> {
        @Override protected void reduce (OrderBean key, Iterable < NullWritable > val, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
        }
    }
}