/*
 * Ribosomal Database Project II
 * Copyright 2009 Michigan State University Board of Trustees
 */
package edu.msu.cme.rdp.hadoop.utils;

import edu.msu.cme.pyro.cluster.Clustering;
import edu.msu.cme.pyro.cluster.dist.ThinEdge;
import edu.msu.cme.pyro.cluster.utils.ClusterFactory;
import edu.msu.cme.pyro.cluster.io.ClusterFileOutput;
import edu.msu.cme.pyro.cluster.io.ClusterOutput;
import edu.msu.cme.pyro.cluster.io.EdgeReader;
import edu.msu.cme.pyro.cluster.io.EdgeWriter;
import edu.msu.cme.pyro.cluster.utils.AbstractClusterFactory;
import edu.msu.cme.pyro.derep.IdMapping;
import edu.msu.cme.pyro.derep.SampleMapping;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 *
 * @author farrisry
 */
public class HadoopClustering {

    private static void printUsageAndExit() {
        System.err.println("usage: [-m merges_file | stepSize idMapping sampleMapping clustFileName] partitionFile0 partitionFile1 ...");
        System.exit(1);
    }

    /**
     * @param args the command line arguments
     */
    public static void main(String... args) throws IOException {
        Configuration conf = new Configuration();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (args.length == 0) {
            printUsageAndExit();
        }

        ClusterOutput clusterOut = null;
        ClusterFactory f = null;
        float step = -1;
        int fIndex = -1;

        if (args[0].equals("-m")) {
            f = new ClusterFactory(new File(args[1]));
            fIndex = 2;
            clusterOut = new ClusterOutput() {

                public void printClusters(AbstractClusterFactory factory, int step) {
                }

                public void close() {
                }

            };
        } else if (args.length > 4) {
            step = Float.parseFloat(args[0]);
            IdMapping<Integer> idMapping = IdMapping.fromFile(new File(args[1]));
            SampleMapping mappingFile = SampleMapping.fromFile(new File(args[2]));
            String clusterFileName = args[3];

            PrintStream out = new PrintStream(clusterFileName);
            clusterOut = new ClusterFileOutput(idMapping, mappingFile, out);

            f = new ClusterFactory();
            fIndex = 4;
        } else {
            printUsageAndExit();
        }

        List<Path> files = new ArrayList<Path>();

        for (int i = fIndex; i < args.length; i++) {
            files.add(new Path(args[i]));
        }

        EdgeReader reader = new HDFSEdgeReader(conf, files);

        Clustering.doCompleteLinkage(f, reader, step, clusterOut);

        if(clusterOut != null) {
            clusterOut.close();
        }

	f.finish();
    }

    public static void dumpDists(String... args) throws IOException {

        Configuration conf = new Configuration();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();


        if (args.length < 1) {
            System.err.println("usage: hdfs-partion-file ...");
            System.exit(1);
        }

        List<Path> files = new ArrayList<Path>();

        for (int i = 0; i < args.length; i++) {
            files.add(new Path(args[i]));
        }

        EdgeReader reader = new HDFSEdgeReader(conf, files);
        ThinEdge edge;
        int lastEdge = Integer.MIN_VALUE;

        while ((edge = reader.nextThinEdge()) != null) {
            if (edge.getDist() < lastEdge) {
                System.out.println(" LAST > NEW: " + lastEdge + " : " + edge.getSeqi() + " " + edge.getSeqj() + " " + edge.getDist());
            }
            System.out.println(edge.getSeqi() + " " + edge.getSeqj() + " " + edge.getDist());
            lastEdge = edge.getDist();
        }
    }

    public static void dumpBinDists(String... args) throws IOException {

        Configuration conf = new Configuration();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();


        if (args.length < 2) {
            System.err.println("usage: outfile hdfs-partion-file ...");
            System.exit(1);
        }

        List<Path> files = new ArrayList<Path>();

        for (int i = 1; i < args.length; i++) {
            files.add(new Path(args[i]));
        }

        EdgeReader reader = new HDFSEdgeReader(conf, files);
        ThinEdge edge;
        int lastEdge = Integer.MIN_VALUE;
        EdgeWriter writer = new EdgeWriter(new File(args[0]));

        long readEdges = 0;
        try {
            while ((edge = reader.nextThinEdge()) != null) {
                if (edge.getDist() < lastEdge) {
                    System.err.println(" LAST > NEW: " + lastEdge + " : " + edge.getSeqi() + " " + edge.getSeqj() + " " + edge.getDist());
                }

                writer.writeEdge(edge);
                lastEdge = edge.getDist();

                readEdges++;
                if (readEdges % 10000000 == 0) {
                    System.out.printf("Dumped %n edges", readEdges);
                }
            }
            writer.close();
        } catch (Exception e) {
            writer.close();
            new File(args[0]).delete();
        }
    }
}
