import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

public class CommonFriends {

    public static class Map
            extends Mapper<LongWritable, Text, Text, Text> {
        /**
         * INPUT
         * <User><TAB><Friends>
         */
        // type of output key
        private Text word = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] friends = value.toString().split("\t");
            if (friends.length > 1) {
                String usr1 = friends[0];
                List<String> values = Arrays.asList(friends[1].split(","));
                // now iterate over list of the friends and compare the value of friend 1 & 2
                for (String friend : values) {
                    int u1 = Integer.parseInt(usr1);
                    int u2 = Integer.parseInt(friend);
                    if (u1 < u2) {
                        word.set("( " + usr1 + "," + friend + " )");
                    }
                    else {
                        word.set("( " + friend + "," + usr1 + " )");
                    }
                    context.write(word, new Text(friends[1]));
                }
            }

        }
    }
    static Text[] returnPairs = {new Text("( 0,1 )"),new Text("( 20,28193 )"), new Text("( 1,29826 )"),new Text("( 6222,19272 )"),new Text("( 28041,28056 )")};
    static HashSet<Text> returnSet = new HashSet<Text>(Arrays.asList(returnPairs));
    public static class Reduce
            extends Reducer<Text,Text,Text,Text> {
        private Text result = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashSet<String> map = new HashSet<>();
            String ret = "";
            for (Text friends : values) {
                List<String> temp = Arrays.asList(friends.toString().split(","));
                for (String friend : temp) {
                    if (map.contains(friend)){
                        ret = ret +" "+ friend;
                    }
                    else{
                        map.add(friend);
                    }
                }
            }
            //if(!ret.equals("")){
                if(returnSet.contains(key)){
                    result.set(new Text("( "+ret+" )"));
                    context.write(key, result);
                }
           // }
        }
    }

    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 2) {
            System.err.println("Usage: CommonFriends <in> <out>");
            System.exit(2);
        }

        // create a job with name "commonfriends"
        Job job = new Job(conf, "commonfriends");
        job.setJarByClass(CommonFriends.class);
        job.setMapperClass(CommonFriends.Map.class);
        job.setReducerClass(CommonFriends.Reduce.class);

        // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);


        // set map key type
        job.setMapOutputKeyClass(Text.class);
        // set map value type
        job.setMapOutputValueClass(Text.class);
        // set output key
        job.setOutputKeyClass(Text.class);
        // set output value
        job.setOutputValueClass(Text.class);

        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
