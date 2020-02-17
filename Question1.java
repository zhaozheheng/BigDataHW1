package question_1;

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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Question1 {

    static final String[] outputData = new String[]{"0,1", "20,28193", "1,29826", "6222,19272", "28041,28056"};

    static boolean checkInOutputList (String pair) {
        for (String output : outputData) {
            if (pair.equals(output)) {  //use equals instead of == for String
                //System.out.println("Got it!!!!!!");
                return true;
            }
        }
        return false;
    }

    public static class FriendMapper extends Mapper<LongWritable, Text, Text, Text> {

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

    public static class FriendReducer extends Reducer<Text, Text, Text, Text> {

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
            if (checkInOutputList(key.toString())) {
                if (tmp.length() != 0) {
                    commonFriends.set(tmp.substring(0, tmp.lastIndexOf(",")));
                    context.write(key, commonFriends);
                } else {
                    commonFriends.set("No Mutual Friends");
                    context.write(key, commonFriends);
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Mutal_Friends");
        job.setJarByClass(Question1.class);
        job.setMapperClass(FriendMapper.class);
        job.setReducerClass(FriendReducer.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]  + "/soc-LiveJournal1Adj.txt"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
