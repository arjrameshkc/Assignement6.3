# Assignement6.3
package mapreduce; import java.io.IOException;

import org.apache.hadoop.conf.Configuration; import org.apache.hadoop.fs.Path; import org.apache.hadoop.io.IntWritable; import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job; import org.apache.hadoop.mapreduce.Mapper; import org.apache.hadoop.mapreduce.Reducer; import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TotalUnits {

public static class MapperClass extends Mapper<Text, Text, Text, IntWritable> { public void map(Text key, Text tvRecord, Context con) throws IOException, InterruptedException { String[] words = tvRecord.toString().split("|"); String CompName= words[0]; try { int count = Integer.parseInt(CompName); con.write(new Text(CompName), new IntWritable(count)); } catch (Exception e) { e.printStackTrace(); } } }

public static class PartitionerClass extends Partitioner<Text,Text>{ public getPartition(Text key,Text value, Int numReduceTasks){ String[] comp = value.tostring(). split("|"); Text firstLetter = new Text(); IntWritable wordlength = new Intwritable(); for(String compn : comp)){ if (compn.length()> 0){ firstLetter.set(String.valueOf(compn.charAt(0)); wordLength.set(word.length()); context.write(firstLetter, wordLength); if(firstLetter >= 'A' && firstLetter <= 'F' ){ return 0; } else if(firstLetter >= 'G' && firstLetter <= 'L' ){ return 1; } else if( firstLetter >= 'M' && firstLetter <= 'R'){ return 2; } else return 3;

} } }

public static class ReducerClass extends Reducer<Text, IntWritable, Text, Text> { public void reduce(Text key, Iterable valueList, Context con) throws IOException, InterruptedException { try {

int tcount = 0;
int count= 0; for (IntWritable var : valueList) { count = var.get(); tcount++; } String out = "Count: " + tcount; con.write(key, new Text(out)); } catch (Exception e) { e.printStackTrace(); } } }

public static void main(String[] args) { Configuration conf = new Configuration(); try { Job job = Job.getInstance(conf, "TotalUnits"); job.setJarByClass(AverageAndTotalSalaryCompute.class); job.setMapperClass(MapperClass.class); job.setReducerClass(ReducerClass.class); job.setOutputKeyClass(Text.class); job.setOutputValueClass(IntWritable.class); job.setNumReduceTasks(4); Path p1 = new Path(args[0]); Path p2 = new Path(args[1]); FileInputFormat.addInputPath(job, p1); FileOutputFormat.setOutputPath(job, p2); FileInputFormat.addInputPath(job, p1); FileOutputFormat.setOutputPath(job, p2); System.exit(job.waitForCompletion(true) ? 0 : 1); } catch (IOException e) { e.printStackTrace(); } catch (ClassNotFoundException e) { e.printStackTrace(); } catch (InterruptedException e) { e.printStackTrace(); }

} }
