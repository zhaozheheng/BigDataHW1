package question_4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Question4 {


    //static final SimpleDateFormat sd = new SimpleDateFormat("MM/dd/yyyy");
    static Date date = new Date();
    static LocalDate localDate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
    static final int thisYear = localDate.getYear();

    public static class AverAgeMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

        private Text userIDWithAddress = new Text();
        private LongWritable averAgeOfFri = new LongWritable();
        private Map<Integer, String> userAddress = new HashMap<Integer, String>();
        private Map<Integer, Integer> userAge = new HashMap<Integer, Integer>();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            String user = line[0];
            int sum = 0;
            int ave;

            if (line.length == 2) {
                String[] friendsList = line[1].split(",");
                for (String friend : friendsList) {
                    sum += userAge.get(Integer.parseInt(friend));
                }
                ave = sum / friendsList.length;
                userIDWithAddress.set(user + "," + userAddress.get(Integer.parseInt(user)));
                averAgeOfFri.set(ave);
                context.write(averAgeOfFri, userIDWithAddress);
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
                userAddress.put(Integer.parseInt(details[0]), details[3] + "," + details[4] + "," + details[5] + ",");
                String userBirthDate = details[details.length - 1];
                String userBirthYear = userBirthDate.substring(userBirthDate.length() - 4, userBirthDate.length());
                int age = thisYear - Integer.parseInt(userBirthYear);
                userAge.put(Integer.parseInt(details[0]), age);
                //System.out.println(details[0]);
            }
        }
    }



    public static class AverAgeReducer extends Reducer<LongWritable, Text, Text, NullWritable> {

        private int idx = 0;
        private Text res = new Text();

        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                if (idx < 15) {
                    idx++;
                    String nameWithAddress = val.toString();
                    res.set(nameWithAddress + key.toString());
                    context.write(res, NullWritable.get());
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("ARGUMENT", args[0]);
        Job job = new Job(conf, "User_Address_AverAge");
        job.setJarByClass(Question4.class);
        job.setMapperClass(AverAgeMapper.class);
        job.setReducerClass(AverAgeReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setSortComparatorClass(LongWritable.DecreasingComparator.class);

        FileInputFormat.addInputPath(job, new Path(args[0]  + "/soc-LiveJournal1Adj.txt"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
