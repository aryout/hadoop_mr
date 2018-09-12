import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by 97390 on 9/12/2018.
 */
public class StepSecond {
    static class SecondMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] friend_users = line.split("\t");
            String friend = friend_users[0];
            String[] users = friend_users[1].split(",");
            Arrays.sort(users);
            for (int i = 0; i < users.length - 1; i++) {
                for (int j = i + 1; j < users.length; j++) {
                    // 这两人有共同好友
                    context.write(new Text(users[i] + "-" + users[j]), new Text(friend));
                }
            }
        }
    }

    static class SecondReducer extends Reducer<Text, Text, Text , Text> {
        @Override protected void reduce (Text user_user, Iterable < Text > friends, Context context) throws IOException, InterruptedException {
            StringBuffer buf = new StringBuffer();
            for (Text friend : friends) {
                buf.append(friend).append(" ");
            }
            context.write(user_user, new Text(buf.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(StepSecond.class);
        job.setMapperClass(SecondMapper.class);
        job.setReducerClass(SecondReducer.class);

        // reduce
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        /*job.submit();*/
        boolean res = job.waitForCompletion(true);
        //System.exit(res?0:1);
    }

}
