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
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class MaxAge {

    public static class joinMap
            extends Mapper<LongWritable, Text, Text, Text> {
        /**
         * INPUT
         * <User><TAB><Friends>
         */
        // type of output key
        //private Text word = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] friends = value.toString().split("\t");
            if (friends.length > 1) {
                String usr1 = friends[0];
                List<String> values = Arrays.asList(friends[1].split(","));
                // now iterate over list of the friends and compare the value of friend 1 & 2
                for (String friend : values) {
                    context.write(new Text(usr1), new Text(friend));
                }
            }


        }
    }
    public static class joinReduce
            extends Reducer<Text,Text, NullWritable,Text>{
        private static HashMap<String,UserWritable> map = new HashMap<>();
        private BufferedReader Reader;
        enum COUNTER {
            RECORD_COUNT, FILE_EXISTS, FILE_NOT_FOUND, SOME_OTHER_ERROR
        }

        @Override
        protected void setup(Context context) throws IOException{
            Configuration conf = new Configuration();
            FileSystem filesystem = FileSystem.get(conf);
            URI[] cacheFilesLocal = context.getCacheFiles();
            Path path = new Path(cacheFilesLocal[0].getPath());
            loadToHashMap(filesystem.open(path),context);
        }

        private void loadToHashMap(FSDataInputStream path, Reducer.Context context) throws IOException {
            String strLineRead = "";
            try {
                Reader = new BufferedReader(new InputStreamReader(path));
                // Read each line, split and load to HashMap
                while ((strLineRead = Reader.readLine()) != null) {
                    String[] Line = strLineRead.split(",");
                    if(Line.length == 10){
                        map.put(Line[0],new UserWritable(Line));
                    }

                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                context.getCounter(COUNTER.FILE_NOT_FOUND).increment(1);
            } catch (IOException e) {
                context.getCounter(COUNTER.SOME_OTHER_ERROR).increment(1);
                e.printStackTrace();
            }finally {
                if (Reader != null) {
                    Reader.close();
                }
            }
        }
//        private Text result = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int currentMaxAge = -1;
            UserWritable oldestFriend = null;
            for(Text value : values){
                UserWritable frienddata = map.get(value.toString());
                if(frienddata.getAge() > currentMaxAge){
                    currentMaxAge = frienddata.getAge();
                    oldestFriend = frienddata;
                }
            }
            if(oldestFriend != null){
                context.write(NullWritable.get(),new Text(key.toString()+"\t"+oldestFriend.toString()));
            }
        }
    }

    public static class rankMap
            extends Mapper<LongWritable, Text, LongWritable, Text> {
        /**
         * INPUT
         * <User><TAB><Friends>
         */
        // type of output key
        //private Text word = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split(",");
            if (data.length == 4) {
                context.write(new LongWritable(Integer.parseInt(data[3])), value);
            }
        }
    }

    public static class rankReduce
            extends Reducer<LongWritable,Text,NullWritable,Text>{
        int countRecord = 0;
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text record : values) {
                if (countRecord < 10) {
                    countRecord++;
                    context.write(NullWritable.get(), new Text(record + "," + key.toString()));
                } else {
                    break;
                }
            }

        }

    }

    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
//        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//        // get all args
//        if (otherArgs.length != 2) {
//            System.err.println("Usage: CommonFriends <in><in2> <out><out>");
//            System.exit(2);
//        }

        // create a job with name "commonfriends"
        Job job = Job.getInstance(conf, "max_age_friends");
        job.addCacheFile(new Path(args[1]).toUri());
        job.setJarByClass(MaxAge.class);
        job.setMapperClass(MaxAge.joinMap.class);
        job.setReducerClass(MaxAge.joinReduce.class);

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
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        if (!job.waitForCompletion(true)) {
            System.exit(1);
        }

        {
            Configuration conf2 = new Configuration();
//            String[] otherArgs2 = new GenericOptionsParser(conf2, args).getRemainingArgs();
//            // get all args
//            if (otherArgs2.length != 3) {
//                System.err.println("Usage: CommonFriends <in> <out1> <out2>");
//                System.exit(2);
//            }
            // create a job with name "commonfriends"
            Job countjob = Job.getInstance(conf2, "Top_max_age_friends");
            countjob.setJarByClass(MaxAge.class);
            countjob.setMapperClass(MaxAge.rankMap.class);
            countjob.setReducerClass(MaxAge.rankReduce.class);

            // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);

            // set map key type
            countjob.setMapOutputKeyClass(LongWritable.class);
            // set map value type
            countjob.setMapOutputValueClass(Text.class);
            countjob.setSortComparatorClass(LongWritable.DecreasingComparator.class);
            // set output key
            countjob.setOutputKeyClass(NullWritable.class);
            // set output value
            countjob.setOutputValueClass(Text.class);

            //set the HDFS path of the input data
            FileInputFormat.addInputPath(countjob, new Path(args[2]));
            // set the HDFS path for the output
            FileOutputFormat.setOutputPath(countjob, new Path(args[3]));
            //Wait till job completion
            System.exit(countjob.waitForCompletion(true) ? 0 : 1);
        }
    }

}
