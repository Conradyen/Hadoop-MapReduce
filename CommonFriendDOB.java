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

import java.io.*;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class CommonFriendDOB {
    public static class commonFriendMap
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
                        word.set(usr1 + "," + friend);
                    }
                    else {
                        word.set(friend + "," + usr1);
                    }
                    context.write(word, new Text(friends[1]));
                }
            }

        }
    }
    public static class commonFriendReduce
            extends Reducer<Text,Text, NullWritable,Text> {
        private Text result = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashSet<String> map = new HashSet<>();
            String ret = "";
            for (Text friends : values) {
                List<String> temp = Arrays.asList(friends.toString().split(","));
                for (String friend : temp) {
                    if (map.contains(friend)){
                        if(ret != ""){
                            ret = ret +","+ friend;
                        }else{
                            ret += friend;
                        }
                    }
                    else{
                        map.add(friend);
                    }
                }
            }
            if(!ret.equals("")){
                result.set(new Text(key.toString()+"#"+ret));
                context.write(NullWritable.get(), result);
            }
        }
    }

    public static class nameMap
            extends Mapper<LongWritable, Text, Text, Text>{
        private static HashMap<String,User> map = new HashMap<>();
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

        private void loadToHashMap(FSDataInputStream path, Context context) throws IOException {
            String strLineRead = "";

            try {
                Reader = new BufferedReader(new InputStreamReader(path));
                // Read each line, split and load to HashMap
                while ((strLineRead = Reader.readLine()) != null) {
                    String[] Line = strLineRead.split(",");
                    if(Line.length == 10){
                        map.put(Line[0],new User(Line));
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


        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.getCounter(COUNTER.RECORD_COUNT).increment(1);
            String ret = "";
            String[] inval = value.toString().split("#");
            if(inval.length > 1){
                String[] friendIDs = inval[1].split(",");
                for(String id : friendIDs){
                    User info = map.get(id);
                    if(info != null){
                        ret = ret +" "+info.firstname+":"+info.dateOfBirth;
                    }
                }
            }
            context.write(new Text(inval[0]),new Text(ret));
        }
    }


    public static class nameReduce
            extends Reducer<Text,Text, Text,Text>{
//        private Text result = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String ret = "";
            for(Text v : values){
                ret += " " + v.toString();
            }
            context.write(key,new Text(ret));
        }

    }

    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 4) {
            System.err.println("Usage: CommonFriends <in><in> <out><out>");
            System.exit(2);
        }

        // create a job with name "commonfriends"
        Job job = Job.getInstance(conf, "commonfriends");
        job.setJarByClass(CommonFriendDOB.class);
        job.setMapperClass(CommonFriendDOB.commonFriendMap.class);
        job.setReducerClass(CommonFriendDOB.commonFriendReduce.class);

        // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);


        // set map key type
        job.setMapOutputKeyClass(Text.class);
        // set map value type
        job.setMapOutputValueClass(Text.class);
        // set output key
        job.setOutputKeyClass(NullWritable.class);
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
            Job namejob = Job.getInstance(conf2, "name_of_friends");
            namejob.addCacheFile(new Path(args[1]).toUri());
            namejob.setJarByClass(CommonFriendDOB.class);
            namejob.setMapperClass(CommonFriendDOB.nameMap.class);
            namejob.setReducerClass(CommonFriendDOB.nameReduce.class);

            // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);


            // set map key type
            namejob.setMapOutputKeyClass(Text.class);
            // set map value type
            namejob.setMapOutputValueClass(Text.class);
            // set output key
            namejob.setOutputKeyClass(Text.class);
            // set output value
            namejob.setOutputValueClass(Text.class);

            //set the HDFS path of the input data
            FileInputFormat.addInputPath(namejob, new Path(args[2]));
            // set the HDFS path for the output
            FileOutputFormat.setOutputPath(namejob, new Path(args[3]));
            //Wait till job completion
            System.exit(namejob.waitForCompletion(true) ? 0 : 1);
        }

    }

}
