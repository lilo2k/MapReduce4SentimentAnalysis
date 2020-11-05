package myproject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

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
import org.apache.hadoop.util.StringUtils;

public class SentimentWordCount {
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

    private static final String BRAND = "Alice";

    private static Set<String> goodWords = new HashSet<String>();
    private static Set<String> badWords = new HashSet<String>();

    // map
    // Object = Input Key, Text = input Value, Text = Output of Map Process,
    // IntWritable = Output Value = 1 Always <in this case> 1 . for each word
    public static class TokenizeMapper extends Mapper<Object, Text, Text, IntWritable> {

        @Override
        protected void setup(Mapper<Object, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            super.setup(context);
        }

        @Override
        // we are getting a line of text at a time as input
        // context is the way in which the key-value pairs is spit out.
        public void map(Object inputKey, Text inputValue, Context context) throws IOException, InterruptedException {

            // pick up lines related to the BRAND
            if (!Pattern.compile(Pattern.quote(BRAND), Pattern.CASE_INSENSITIVE).matcher(inputValue.toString())
                    .find()) {
                return;
            }

            IntWritable valueOne = new IntWritable(1);

            StringTokenizer stringTokenizer = new StringTokenizer(inputValue.toString());

            Text keyWord = new Text();

            while (stringTokenizer.hasMoreTokens()) {
                // removes all non alphabets, and any special characters
                keyWord.set(stringTokenizer.nextToken().toLowerCase().replaceAll(NON_ALPHABET, ""));

                context.write(keyWord, valueOne);
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
            int count = 0;

            for (IntWritable valueOne : listOfOnes) {
                count++;
            }
            IntWritable output = new IntWritable(count);
            contextWriteAggregateOutput.write(term, output);
        }
    }

    // main
    public static void main(String... args) throws IOException, ClassNotFoundException, InterruptedException {
        // System.out.println( "Hello World!" );

        Configuration conf = new Configuration();

        // String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // if (otherArgs.length != 4) {
        // System.err.println("* * * Needs more arguments....usage : WordCount <input
        // file> <output folder> <good word list> <bad word list>");
        // System.exit(2);
        // }

        parsePositive(args[2]);
        parseNegative(args[3]);

        Job job = Job.getInstance(conf, WORD_COUNT);
        job.setJarByClass(SentimentWordCount.class);

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

        boolean isSuccess_JobStatus = job.waitForCompletion(true); // submits the job

        if (isSuccess_JobStatus) {
            System.exit(0); // Exit with Success code
            System.out.println(COMPLETED_THE_MAP_REDUCE_SUCCESSFULLY);
        } else {
            System.exit(1); // Exit with Failure code # 1
            System.out.println(EXITED_WITH_ERRORS);
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

    // Parse the positive words to match and capture during Map phase.
    private static void parsePositive(String goodWordsUri) {
        try {
            BufferedReader fis = new BufferedReader(new FileReader(new File(goodWordsUri)));
            String goodWord;
            while ((goodWord = fis.readLine()) != null) {
                goodWords.add(goodWord);
            }
        } catch (IOException ioe) {
            System.err.println("Caught exception parsing cached file '" + goodWords + "' : "
                    + StringUtils.stringifyException(ioe));
        }
    }

    // Parse the negative words to match and capture during Reduce phase.
    private static void parseNegative(String badWordsUri) {
        try {
            BufferedReader fis = new BufferedReader(new FileReader(new File(badWordsUri)));
            String badWord;
            while ((badWord = fis.readLine()) != null) {
                badWords.add(badWord);
            }
        } catch (IOException ioe) {
            System.err.println("Caught exception while parsing cached file '" + badWords + "' : "
                    + StringUtils.stringifyException(ioe));
        }
    }
}
