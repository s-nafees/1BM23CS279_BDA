import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MeanMaxTemperatureByMonth {

    public static class MeanMaxMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        private static final int MISSING = 9999;
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            if (line.length() < 93) return;
            String year  = line.substring(15, 19);
            String month = line.substring(19, 21);
            String day   = line.substring(21, 23);
            String tempStr = line.substring(87, 92).trim();
            int airTemp;
            try { airTemp = Integer.parseInt(tempStr); }
            catch (NumberFormatException e) { return; }
            char quality = line.charAt(92);
            if (airTemp == MISSING) return;
            if ("01459".indexOf(quality) == -1) return;
            context.write(new Text(year + "-" + month),
                          new Text(day + ":" + airTemp));
        }
    }

    public static class MeanMaxReducer
            extends Reducer<Text, Text, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Map<String, Integer> dailyMax = new HashMap<>();
            for (Text val : values) {
                String s   = val.toString();
                int colon  = s.indexOf(':');
                String day = s.substring(0, colon);
                int temp   = Integer.parseInt(s.substring(colon + 1));
                dailyMax.merge(day, temp, Math::max);
            }
            if (dailyMax.isEmpty()) return;
            long sum = 0; int count = 0;
            for (int m : dailyMax.values()) { sum += m; count++; }
            double mean = (double) sum / (count * 10.0);
            context.write(key, new DoubleWritable(
                    Math.round(mean * 100.0) / 100.0));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Mean Max Temperature By Month");
        job.setJarByClass(MeanMaxTemperatureByMonth.class);
        job.setMapperClass(MeanMaxMapper.class);
        job.setReducerClass(MeanMaxReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
