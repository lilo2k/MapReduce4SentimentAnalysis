package myproject;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
import org.apache.log4j.Logger;

public class SentimentWordCount {
    private static final Logger LOG = Logger.getLogger(SentimentWordCount.class);
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

    // private static final String BRAND = "mcdonald\'?s";
    private static final String BRAND = "starbucks";

    private static final String POSITIVE = "positive";
    /**
    *
    */
    private static final String NEGATIVE = "negative";
    /**
     *
     */
    private static final String SI = "SI";

    // map
    // Object = Input Key, Text = input Value, Text = Output of Map Process,
    // IntWritable = Output Value = 1 Always <in this case> 1 . for each word
    public static class TokenizeMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static Set<String> goodWords = new HashSet<String>();
        private static Set<String> badWords = new HashSet<String>();

        /**
             * Emoticon
             */
        private static String positiveEmoticons =  
            "\\Q:)\\E|\\Q:]\\E|\\Q:}\\E|\\Q:o)\\E|\\Q:o]\\E|\\Q:o}\\E"
            +"|\\Q:-]\\E|\\Q:-)\\E|\\Q:-}\\E|\\Q=)\\E|\\Q=]\\E|\\Q=}\\E"
            +"|\\Q=^]\\E|\\Q=^)\\E|\\Q=^}\\E|\\Q:B\\E|\\Q:-D\\E|\\Q:-B\\E"
            +"|\\Q:^D\\E|\\Q:^B\\E|\\Q=B\\E|\\Q=^B\\E|\\Q=^D\\E|\\Q:’)\\E"
            +"|\\Q:’]\\E|\\Q:’}\\E|\\Q=’)\\E|\\Q=’]\\E|\\Q=’}\\E|\\Q<3\\E"
            +"|\\Q^.^\\E|\\Q^-^\\E|\\Q^_^\\E|\\Q^^\\E|\\Q:*\\E|\\Q=*\\E"
            +"|\\Q:-*\\E|\\Q;)\\E|\\Q;]\\E|\\Q;}\\E|\\Q:-p\\E|\\Q:-P\\E"
            +"|\\Q:-b\\E|\\Q:^p\\E|\\Q:^P\\E|\\Q:^b\\E|\\Q=P\\E"
            +"|\\Q=p\\E|\\Q\\o\\\\E|\\Q/o/\\E|\\Q:P\\E|\\Q:p\\E|\\Q:b\\E|\\Q=b\\E"
            +"|\\Q=^p\\E|\\Q=^P\\E|\\Q=^b\\E|\\Q\\o/\\E"
            ;
        private static String negativeEmoticons = "\\Q:|\\E|\\Q=|\\E|\\Q:-|\\E|\\Q>.<\\E|\\Q><\\E|\\Q>_<\\E|\\Q:o\\E"
            // +"|\\Q:0\\E"
            +"|\\Q=O\\E|\\Q:@\\E"
            +"|\\Q=@\\E|\\Q:^o\\E|\\Q:^@\\E|\\Q-.-\\E"
            +"|\\Q-.-’\\E|\\Q-_-\\E|\\Q-_-’\\E"
            +"|\\Q:x\\E|\\Q=X\\E|\\Q:#\\E"
            +"|\\Q=#\\E|\\Q:-x\\E|\\Q:-@\\E"
            +"|\\Q:-#\\E|\\Q:^x\\E|\\Q:^#\\E"
            ;

        @Override
        protected void setup(Mapper<Object, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            super.setup(context);

            // parsePositive("pos-words.txt");
            // System.out.println("parsePositive done");
            // parseNegative("neg-words.txt");
            // System.out.println("parseNegative done");

            // URL url = this.getClass().getClassLoader().getResource("pos-words.txt");
            // File file = new File(url.getPath());

            InputStream in = getClass().getResourceAsStream("pos-words.txt"); 
            BufferedReader fis = new BufferedReader(new InputStreamReader(in));
            // BufferedReader fis = new BufferedReader(new FileReader(file));
            String goodWord;
            while ((goodWord = fis.readLine()) != null) {
                goodWords.add(goodWord);
            }
            fis.close();

            in = getClass().getResourceAsStream("neg-words.txt"); 
            fis = new BufferedReader(new InputStreamReader(in));
            // url = this.getClass().getClassLoader().getResource("neg-words.txt");
            // file = new File(url.getPath());
            // fis = new BufferedReader(new FileReader(file));
            String badWord;
            while ((badWord = fis.readLine()) != null) {
                badWords.add(badWord);
            }
            fis.close();
        }

        /**
         *
         */
        @Override
        // we are getting a line of text at a time as input
        // context is the way in which the key-value pairs is spit out.
        public void map(Object inputKey, Text inputText, Context context) throws IOException, InterruptedException {
            // System.out.println("MAP");

            String inputString = inputText.toString();
            
            // Opt out lines not related to the BRAND
            if (!Pattern.compile(BRAND, Pattern.CASE_INSENSITIVE ).matcher(inputString).find()) {
                return;
            }
            // System.out.println("Found: " + BRAND + " in " + inputValue);

            IntWritable valueOne = new IntWritable(1);
            Text keyWord = new Text();
            
            
            StringTokenizer stringTokenizer = new StringTokenizer(inputString);

            // "4","2071031926","Sun Jun 07 18:43:04 PDT 2009","NO_QUERY","zoeeeyeeee","Getting sushi with @nicurnmama "
            while (stringTokenizer.hasMoreTokens()) {
                // removes all non alphabets, and any special characters
                
                // keyWord.set(stringTokenizer.nextToken().toLowerCase().replaceAll(NON_ALPHABET,"") + goodWords.size());
                // context.write(keyWord, valueOne);
                
                String word = stringTokenizer.nextToken().toLowerCase();//.replaceAll(NON_ALPHABET, "");
                
                if (Pattern.compile(positiveEmoticons).matcher(word).find()) {
                    keyWord.set(POSITIVE);
                    context.write(keyWord, valueOne);
                }
    
                if (Pattern.compile(negativeEmoticons).matcher(word).find()) {
                    keyWord.set(NEGATIVE);
                    context.write(keyWord, valueOne);
                }

                // Filter and count "good" words.
                if (goodWords.contains(word.replaceAll(NON_ALPHABET, ""))) {
                    // context.getCounter(Gauge.POSITIVE).increment(1);
                    // System.out.println(word);
                    // System.out.println("Word: " + word);
                    keyWord.set(POSITIVE);
                    context.write(keyWord, valueOne);
                }

                // Filter and count "bad" words.
                if (badWords.contains(word.replaceAll(NON_ALPHABET, ""))) {
                    // context.getCounter(Gauge.NEGATIVE).increment(1);
                    // System.out.println(word);
                    // System.out.println("Word: " + word);
                    keyWord.set(NEGATIVE);
                    context.write(keyWord, valueOne);
                }
            }
        }
    }

    // reduce
    // The input of the reducer is the output of the mapper.
    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private static int positiveCount = 0;
        private static int negativeCount = 0;

        // System automatically shuffle-sorts, and gives a iterable list as value to
        // this reducer function.
        // e.g. ("hello",1,1,1,1) hello is key, and 1,1,1,1 is iterable list
        public void reduce(Text key, Iterable<IntWritable> listOfOnes, Context context)
                throws IOException, InterruptedException {
            // System.out.println("REDUCE");
            int count = 0;
            for (IntWritable valueOne : listOfOnes) {
                count++;
            }
            if (key.toString().equals(POSITIVE)) {
                positiveCount = count;
                // System.out.println("POSITIVE count: " + count);

            } else if (key.toString().equals(NEGATIVE)) {
                negativeCount = count;
                // System.out.println("NEGATIVE count: " + count);
            }
            // System.out.println("Key: " + key.toString());

            // Skip writing if the keys are YYYYMM style
            // IntWritable output = new IntWritable(count);
            // context.write(key, output);
        }

        @Override
        protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            // TODO Auto-generated method stub
            // System.out.println("CLEANUP");
            super.cleanup(context);
            Text keyWord = new Text(SentimentWordCount.SI);
            double sentimentIndex = 0.0d;
            // System.out.println("POSITIVE count: " + positiveCount);
            // System.out.println("NEGATIVE count: " + negativeCount);

            sentimentIndex = Math.round((positiveCount - negativeCount) * 100.0 / (positiveCount + negativeCount));
            // System.out.println("SI: " + sentimentIndex);

            IntWritable output = new IntWritable((int) (sentimentIndex));
            context.write(keyWord, output);
        }
    }

    // main
    public static void main(String... args) throws IOException, ClassNotFoundException, InterruptedException {
        // System.out.println( "Hello World!" );
        // BasicConfigurator.configure();

        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        for (String arg : otherArgs) {
            // System.out.println("arg: " + arg);
            System.out.println("arg: " + arg);
        }

        if (otherArgs.length < 2) {
            System.err.println("* * * Needs more arguments....usage : WordCount <input file> <output folder>");
            System.exit(2);
        }

        // parsePositive(args[2]);
        // parseNegative(args[3]);
        // parsePositive("resources/pos-words.txt");
        // System.out.println("parsePositive done");
        // parseNegative("resources/neg-words.txt");
        // System.out.println("parseNegative done");

        Job job = Job.getInstance(conf, WORD_COUNT);
        job.setJarByClass(SentimentWordCount.class);

        job.setMapperClass(TokenizeMapper.class);
        job.setReducerClass(SumReducer.class);

        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        String outputFolderName = createOutputFolderName(args[1]);
        System.out.println("outputFolderName:" + outputFolderName);
        FileOutputFormat.setOutputPath(job, new Path(outputFolderName));
        // FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.out.println("before job start");
        boolean isSuccess_JobStatus = job.waitForCompletion(true); // submits the job

        System.out.println("after job");
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
