package question_3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Question3 {

    public static class MutualFriMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text pairKey = new Text();
        private Text friends = new Text();
        private Map<Integer, String> friendsDetails = new HashMap<Integer, String>();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            String user = line[0];
            if (line.length == 2) {
                String[] friendsList = line[1].split(",");
                String friNameWithState, tmp = "";
                for (String friendID : friendsList) {
                    tmp += friendID + ":" + friendsDetails.get(Integer.parseInt(friendID)) + ",";
                }
                friNameWithState = tmp.substring(0, tmp.lastIndexOf(","));
                for (String friend : friendsList) {
                    String pair = (Integer.parseInt(user) < Integer.parseInt(friend)) ?
                            user + "," + friend : friend + "," + user;

                    pairKey.set(pair);
                    friends.set(friNameWithState);
                    //System.out.println(friNameWithState);
                    context.write(pairKey, friends);
                }
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            Path userData = new Path(conf.get("ARGUMENT") + "/userdata.txt");
            FileSystem fileSystem = FileSystem.get(conf);
            FSDataInputStream in = null;
            in = fileSystem.open(userData);
            InputStreamReader isr = new InputStreamReader(in);
            BufferedReader br = new BufferedReader(isr);
            String line;
            while ((line = br.readLine()) != null) {
                String[] details = line.split(",");
                friendsDetails.put(Integer.parseInt(details[0]), details[1] + ":" + details[5]);
                //System.out.println(details[0]);
            }
        }
    }

    public static class FriDetailsReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> valList = values.iterator();
            StringBuilder sb = new StringBuilder();
            Text commonFriends = new Text();
            Map<String, Integer> count = new HashMap<String, Integer>();

            while (valList.hasNext()) {
                String[] friendsList = valList.next().toString().split(",");
                for (String friend : friendsList) {
                    String[] friNameAndState = friend.split(":");
                    //System.out.println(friNameAndState.length);
                    if (count.containsKey(friNameAndState[0])) {
                        if (friNameAndState.length == 3) {  //user with state
                            sb.append(friNameAndState[1] + ":" + friNameAndState[2] + ",");
                        } else {    //some user does not have state
                            sb.append(friNameAndState[1] + ":" + "N/A" + ",");
                        }
                    } else {
                        count.put(friNameAndState[0], 1);
                    }
                }
            }
            String tmp = sb.toString();
            if (tmp.length() != 0) {
                commonFriends.set(tmp.substring(0, tmp.lastIndexOf(",")));
                context.write(key, commonFriends);
            } else {
                commonFriends.set("No Mutual Friends");
                context.write(key, commonFriends);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("ARGUMENT", args[0]);
        Job job = new Job(conf, "Mutal_Friends_Details");
        job.setJarByClass(Question3.class);
        job.setMapperClass(MutualFriMapper.class);
        job.setReducerClass(FriDetailsReducer.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]  + "/soc-LiveJournal1Adj.txt"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
