package zutsoft.hadoop;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
    public static void main(String[] args) throws Exception {
        String localProjectPath = new File("").getAbsolutePath();
        System.setProperty("hadoop.home.dir", localProjectPath + "/hadoopdir");
        System.load(localProjectPath + "/hadoopdir/bin/hadoop.dll");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "wordcount");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WCMapper.class);
        job.setReducerClass(WCReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(localProjectPath + "/input/test.data"));
        FileOutputFormat.setOutputPath(job, new Path(localProjectPath + "/output/wordcount"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static class WCMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key,
                        Text value,
                        Context context)
                throws IOException, InterruptedException {
            String[] toks = value.toString().trim().split(" ");
            for (int i = 0; i < toks.length; i++) {
                context.write(new Text(toks[i]), new Text("1"));
            }
        }
    }

    private static class WCReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key,
                           Iterable<Text> values,
                           Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (Text value : values) {
                count++;
            }
            context.write(key, new Text(count + ""));
        }
    }
}
