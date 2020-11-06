package myproject;

import java.io.File;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
// import org.apache.log4j.BasicConfigurator;

public class WordCount {
    /**
     *
     */
    private static final String WORD_COUNT = "word count";
    /**
     *
     */
    private static final String COMPLETED_THE_MAP_REDUCE = "Completed the Map Reduce";
    /**
     *
     */
    private static final String EXITED_WITH_ERRORS = "Exited with Errors";
    /**
     *
     */
    private static final String COMPLETED_THE_MAP_REDUCE_SUCCESSFULLY = "Completed the Map Reduce Successfully";
    /**
     *
     */
    private static final String NON_ALPHABET = "[^a-zA-Z]";

    // map
    // Object = Input Key, Text = input Value, Text = Output of Map Process,
    // IntWritable = Output Value = 1 Always <in this case> 1 . for each word
    public static class TokenizeMapper extends Mapper<Object, Text, Text, IntWritable> {

        @Override

        // we are getting a line of text at a time as input
        // context is the way in which the key-value pairs is spit out.
        public void map(Object inputKeyForMapper, Text inputValueForMapper, Context contextSpitOutKeyValuePairs)
                throws IOException, InterruptedException {
            // System.out.println("MAP");
            IntWritable valueOne = new IntWritable(1);

            StringTokenizer stringTokenizer = new StringTokenizer(inputValueForMapper.toString());

            Text keyWord = new Text();

            while (stringTokenizer.hasMoreTokens()) {
                // removes all non alphabets, and any special characters
                keyWord.set(stringTokenizer.nextToken().toLowerCase().replaceAll(NON_ALPHABET, ""));

                contextSpitOutKeyValuePairs.write(keyWord, valueOne);
            }
        }
    }

    // reduce
    // The input of the reducer is the output of the mapper.
    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        // System automatically shuffle-sorts, and gives a iterable list as value to
        // this reducer function.
        // e.g. ("hello",1,1,1,1) hello is key, and 1,1,1,1 is iterable list
        public void reduce(Text term, Iterable<IntWritable> listOfOnes, Context contextWriteAggregateOutput)
                throws IOException, InterruptedException {
            // System.out.println("REDUCE");
            int count = 0;
            
            for (IntWritable valueOne : listOfOnes) {
                count++;
            }
            System.out.println("term:" + term + " : " + count);
            IntWritable output = new IntWritable(count);
            contextWriteAggregateOutput.write(term, output);
        }
    }

    // main
    public static void main(String... args) throws IOException, ClassNotFoundException, InterruptedException {
        // System.out.println( "Hello World!" );
        // BasicConfigurator.configure();

        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // if (otherArgs.length < 2) {
        //     System.err.println("* * * Needs two arguments....usage : WordCount <input_file> <output_folder>");
        //     System.exit(2);
        // }

        Job job = Job.getInstance(conf, WORD_COUNT);
        job.setJarByClass(WordCount.class);

        job.setMapperClass(TokenizeMapper.class);
        job.setReducerClass(SumReducer.class);

        job.setNumReduceTasks(2);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        String outputFolderName = createOutputFolderName(args[1]);
        FileOutputFormat.setOutputPath(job, new Path(outputFolderName));

        System.out.println("before job start");
        boolean isSuccess_JobStatus = job.waitForCompletion(true); // submits the job
        System.out.println("After start");
        
        if (isSuccess_JobStatus) {
            System.out.println(COMPLETED_THE_MAP_REDUCE_SUCCESSFULLY);
            System.exit(0); // Exit with Success code
        } else {
            System.out.println(EXITED_WITH_ERRORS);
            System.exit(1); // Exit with Failure code # 1
        }
        System.out.println(COMPLETED_THE_MAP_REDUCE);
    }

    private static String createOutputFolderName(String folderName) {

        File file = new File(folderName);
        if (file.exists()) {
            return createOutputFolderName(folderName + "1");
        }
        return folderName;
    }
}
