import java.io.IOException;
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

public class AverageTemperatureByYear {

    public static class AvgTempMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        private static final int MISSING = 9999;
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            if (line.length() < 93) return;
            String year = line.substring(15, 19);
            String tempStr = line.substring(87, 92).trim();
            int airTemp;
            try { airTemp = Integer.parseInt(tempStr); }
            catch (NumberFormatException e) { return; }
            char quality = line.charAt(92);
            if (airTemp == MISSING) return;
            if ("01459".indexOf(quality) == -1) return;
            context.write(new Text(year), new Text(airTemp + ",1"));
        }
    }

    public static class AvgTempReducer
            extends Reducer<Text, Text, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0, count = 0;
            for (Text val : values) {
                String[] parts = val.toString().split(",");
                sum   += Long.parseLong(parts[0]);
                count += Long.parseLong(parts[1]);
            }
            if (count == 0) return;
            double avg = (double) sum / (count * 10.0);
            context.write(key, new DoubleWritable(
                    Math.round(avg * 100.0) / 100.0));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Temperature By Year");
        job.setJarByClass(AverageTemperatureByYear.class);
        job.setMapperClass(AvgTempMapper.class);
        job.setReducerClass(AvgTempReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
