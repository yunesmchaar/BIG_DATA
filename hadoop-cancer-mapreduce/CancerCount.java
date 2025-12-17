import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CancerCount {

    // =======================
    // MAPPER
    // =======================
    public static class CancerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text diagnosis = new Text();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // 🔴 Ignorer la ligne d'en-tête (header)
            // ex: id,diagnosis,radius_mean,...
            if (line.toLowerCase().contains("diagnosis")) {
                return;
            }

            // Séparation CSV
            String[] fields = line.split(",");

            // Sécurité (ligne mal formée)
            if (fields.length < 2) {
                return;
            }

            // Colonne diagnosis (M ou B)
            diagnosis.set(fields[1].trim());

            // Émettre (M,1) ou (B,1)
            context.write(diagnosis, one);
        }
    }

    // =======================
    // REDUCER
    // =======================
    public static class CancerReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            context.write(key, new IntWritable(sum));
        }
    }

    // =======================
    // MAIN
    // =======================
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Cancer Diagnosis Count");
        job.setJarByClass(CancerCount.class);

        // Mapper & Reducer
        job.setMapperClass(CancerMapper.class);
        job.setReducerClass(CancerReducer.class);

        // Types de sortie
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // ⚠️ Chemins HDFS (fixes)
        FileInputFormat.addInputPath(job, new Path("/cancer"));
        FileOutputFormat.setOutputPath(job, new Path("/cancer_output"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
