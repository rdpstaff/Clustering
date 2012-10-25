/*
 * Copyright 2009, Michigan State University Board of Trustees
 * 
 * This software is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * The Ribosomal Database Project II and Michigan State University
 * distributes this software in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this software.  If not, see <http://www.gnu.org/licenses/>.
 */

package edu.msu.cme.rdp.hadoop.distance.sampler;

import edu.msu.cme.rdp.alignment.pairwise.rna.DistanceModel;
import edu.msu.cme.rdp.alignment.pairwise.rna.IdentityDistanceModel;
import edu.msu.cme.rdp.hadoop.distance.mapred.keys.DistanceAndComparison;
import edu.msu.cme.rdp.hadoop.distance.mapred.keys.IntDistance;
import edu.msu.cme.rdp.hadoop.distance.mapred.DistanceAndComparisonMapper;
import edu.msu.cme.rdp.hadoop.utils.AlignedIntSeqStore;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Runs the distance sampler.  Must be invoked via the hadoop jar runner.
 *
 * Byte seq file should be in HDFS.
 * Output dir will contain a single partition file containing the distances sampled.
 * The distance cutoff must be the same as what is used to compute the full distances,
 * otherwise the partition files of the full distance computation will not be
 *
 * @author farrisry
 */
public class SamplerMain extends Configured implements Tool {

    public static void main(String ... args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new SamplerMain(), args);
        System.exit(res);
    }
    
    public int run(String[] args) throws Exception {
        if (args.length != 5) {
            System.out.println("usage: <distance_cutoff> <seq_overlap_limit> <number_of_samples_to_run> <byte_seq_file> <samplesFile>");
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        float cutoff = Float.parseFloat(args[0]);
        int overlapLimit = Integer.parseInt(args[1]);
        int numSamples = Integer.parseInt(args[2]);


        Path byteSeqPath = new Path(args[3]);
        URI seqFileCacheURI = new URI(args[3] + "#" + DistanceAndComparisonMapper.SEQ_FILE_NAME);
        
//        Path outputDir = new Path(args[4]);
        Path outputFile = new Path(args[4]);
        Path outputDir = new Path(args[4]);

        JobConf conf = new JobConf(getConf(), this.getClass());
        FileSystem fs = byteSeqPath.getFileSystem(conf);


        // get the number of sequences in the byteSeqFile
        FSDataInputStream seqFileStream = null;
        long numSeqs = -1;
        try {
            seqFileStream = fs.open(byteSeqPath);
            numSeqs = AlignedIntSeqStore.numberOfSeqs(seqFileStream);
        } finally {
            if (seqFileStream != null) {
                seqFileStream.close();
            }
        }

        // set values required by the InputFormat and Mapper
        conf.setLong(SamplerInputFormat.NUM_SEQS_KEY, numSeqs);
        conf.setLong(SamplerInputFormat.SAMPLE_SIZE_KEY, numSamples);
        conf.setLong(SamplerInputFormat.RECORD_SIZE_KEY, 100);
        conf.setInt(DistanceAndComparisonMapper.DISTANCE_CUTOFF_KEY, IntDistance.scale(cutoff));
        conf.setClass(DistanceAndComparisonMapper.DISTANCE_MODEL_KEY, IdentityDistanceModel.class, DistanceModel.class);
        conf.setInt(DistanceAndComparisonMapper.DISTANCE_OVERLAP_LIMIT_KEY, overlapLimit);


        // load the samplesFile and byteSeqFile into the distributed cache
        DistributedCache.addCacheFile(seqFileCacheURI, conf);
        DistributedCache.createSymlink(conf);

        // configure the job
        conf.setMapperClass(DistanceAndComparisonMapper.class);
        conf.setReducerClass(SamplerReducer.class);
        conf.setInt("mapred.reduce.tasks", 1); // there can only be one reducer for sampling

        conf.setInputFormat(SamplerInputFormat.class);
        SequenceFileOutputFormat.setOutputPath(conf, outputDir);

        conf.setMapOutputKeyClass(DistanceAndComparison.class);
        conf.setMapOutputValueClass(NullWritable.class);
        // the setOutputKeyComparatorClass is mislabeled and really should be called setMapOutputKeyComparatorClass
        conf.setOutputKeyComparatorClass(DistanceAndComparison.GroupingComparator.class);

        conf.setOutputValueClass(Text.class);
        conf.setOutputKeyClass(NullWritable.class);

        conf.setJarByClass(DistanceAndComparisonMapper.class);

        JobClient.runJob(conf);
        return 0;
    }
}
