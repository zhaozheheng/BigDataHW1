package question_2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class Question2 {

    //1st mapper
    public static class topMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text pairKey = new Text();
        private Text friends = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            String user = line[0];
            if (line.length == 2) {
                String[] friendsList = line[1].split(",");
                for (String friend : friendsList) {
                    String pair = (Integer.parseInt(user) < Integer.parseInt(friend)) ?
                            user + "," + friend : friend + "," + user;

                        pairKey.set(pair);
                        friends.set(line[1]);
                        context.write(pairKey, friends);
                }
            }
        }
    }

    //1st reducer
    public static class topReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> valList = values.iterator();
            StringBuilder sb = new StringBuilder();
            Text commonFriends = new Text();
            Map<String, Integer> count = new HashMap<String, Integer>();

            while (valList.hasNext()) {
                String[] friendsList = valList.next().toString().split(",");
                for (String friend : friendsList) {
                    if (count.containsKey(friend)) {
                        sb.append(friend + ",");
                    } else {
                        count.put(friend, 1);
                    }
                }
            }
            String tmp = sb.toString();
            if (tmp.length() != 0) {
                commonFriends.set(tmp.substring(0, tmp.lastIndexOf(",")));
                context.write(key, commonFriends);
            }
        }
    }

    //2nd mapper
    public static class topMapper2 extends Mapper<Text, Text, LongWritable, Text> {

        private LongWritable count = new LongWritable();
        private Text output = new Text();

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String friendPair = key.toString();
            //System.out.println(friendPair);

            String[] friendsList = value.toString().split(",");
            int numOfMutualFriends = friendsList.length;
            count.set(numOfMutualFriends);
            output.set(friendPair + "\t" + value.toString());
            context.write(count, output);
        }
    }

    //2nd reducer
    public static class topReducer2 extends Reducer<LongWritable, Text, Text, Text> {

        private int idx = 0;
        private Text friendPair = new Text();
        private Text topDetails = new Text();

        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                if (idx < 10) {
                    idx++;
                    friendPair.set(val.toString().split("\t")[0] + "\t" + key.toString());
                    topDetails.set(val.toString().split("\t")[1]);
                    context.write(friendPair, topDetails);
                }
            }



        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        String inputPath = otherArgs[0];
        String outputPath = otherArgs[1];

        Path tempPath = new Path("/user/zhaozheheng/temp");
        //1st job
        {
            conf = new Configuration();
            Job job = new Job(conf, "mutual_friends");

            job.setJarByClass(Question2.class);
            job.setMapperClass(topMapper.class);
            job.setReducerClass(topReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(inputPath + "/soc-LiveJournal1Adj.txt"));
            FileOutputFormat.setOutputPath(job, tempPath);

            if (!job.waitForCompletion(true))
                System.exit(1);
        }

        //2nd job
        {
            conf = new Configuration();
            Job job = new Job(conf, "top_friends_details");

            job.setJarByClass(Question2.class);
            job.setMapperClass(topMapper2.class);
            job.setReducerClass(topReducer2.class);

            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Text.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(KeyValueTextInputFormat.class);
            job.setSortComparatorClass(LongWritable.DecreasingComparator.class);

            job.setNumReduceTasks(1);
            FileInputFormat.addInputPath(job, tempPath);
            FileOutputFormat.setOutputPath(job, new Path(outputPath));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
}
