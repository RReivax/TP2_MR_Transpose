import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *  Output : result.csv
 */


public class Transpose {

    /***
     * Custom class to hold (index, value), implanting WritableComparable
     */
    public static class LongTextWritable  implements WritableComparable<LongTextWritable> {
        private LongWritable index;
        private Text value;

        public LongTextWritable() {
            index = new LongWritable();
            value = new Text();
        }

        public LongTextWritable(long n_index, Text n_value) {
            index = new LongWritable(n_index);
            value = new Text(n_value);
        }

        public LongTextWritable(LongWritable n_index, Text n_value) {
            index = n_index;
            value = new Text(n_value);
        }

        public LongWritable getIndex() {
            return index;
        }

        public Text getValue() {
            return value;
        }

        @Override
        public int compareTo(LongTextWritable o) {
            return index.compareTo(o.getIndex());
        }

        public void write(DataOutput dataOutput) throws IOException {
            index.write(dataOutput);
            value.write(dataOutput);
        }

        public void readFields(DataInput dataInput) throws IOException {
            index.readFields(dataInput);
            value.readFields(dataInput);
        }

        @Override
        public String toString() {
            return value.toString();
        }
    }

    public static class CoordinateMapper
            extends Mapper<LongWritable, Text, IntWritable, LongTextWritable> {

        private Text cell = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            int y = 0;

            while (itr.hasMoreTokens()) {
                cell.set(itr.nextToken(","));
                context.write(new IntWritable(y), new LongTextWritable(key, cell));
                y++;
            }
        }
    }

    public static class TransposeReducer
            extends Reducer<IntWritable, LongTextWritable, LongWritable, List<Text>> {

        public void reduce(IntWritable key, Iterable<LongTextWritable> values, Context context) throws IOException, InterruptedException {
            ArrayList<LongTextWritable> sortedValues = new ArrayList<>();
            for (LongTextWritable val: values){
                sortedValues.add(new LongTextWritable(new LongWritable(val.getIndex().get()), new Text(val.getValue())));
            }

            sortedValues.sort(Comparator.comparing(LongTextWritable::getIndex));
            List<Text> cells = sortedValues.stream().map(LongTextWritable::getValue).collect(Collectors.toList());

            context.write(new LongWritable(0), cells);
        }
    }

    
	/**
	 * Custom output formating
	 */
	 
	public static class MyTextOutputFormat extends FileOutputFormat<LongWritable, List<Text>> {
        @Override
        public RecordWriter<LongWritable, List<Text>> getRecordWriter(TaskAttemptContext arg0) throws IOException, InterruptedException {
            //get the current path
            Path path = FileOutputFormat.getOutputPath(arg0);
            //create the full path with the output directory plus our filename
            Path fullPath = new Path(path, "result.csv");

            //create the file in the file system
            FileSystem fs = path.getFileSystem(arg0.getConfiguration());
            FSDataOutputStream fileOut = fs.create(fullPath, arg0);

            //create our record writer with the new file
            return new MyCustomRecordWriter(fileOut);
        }
    }

    public static class MyCustomRecordWriter extends RecordWriter<LongWritable, List<Text>> {
        private DataOutputStream out;
        public MyCustomRecordWriter(DataOutputStream stream) {
            out = stream;
        }

        @Override
        public void write(LongWritable longWritable, List<Text> Texts) throws IOException, InterruptedException {
            for (int i=0; i<Texts.size(); i++) {
                if (i>0)
                    out.writeBytes(",");
                out.writeBytes(String.valueOf(Texts.get(i)));
            }
            out.writeBytes("\r\n");
        }

        @Override
        public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
            out.close();
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "transpose");
        job.setJarByClass(Transpose.class);
        job.setMapperClass(CoordinateMapper.class);
        job.setReducerClass(TransposeReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(ArrayList.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(LongTextWritable.class);

        job.setOutputFormatClass(MyTextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}