/*
 * Copyright (C) 2012 Michigan State University <rdpstaff at msu.edu>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package edu.msu.cme.pyro.cluster;

import edu.msu.cme.pyro.cluster.dist.DistanceCalculator;
import edu.msu.cme.pyro.cluster.upgma.CannotLoadMoreEdgesException;
import edu.msu.cme.pyro.cluster.dist.ThickEdge;
import edu.msu.cme.pyro.cluster.dist.ThinEdge;
import edu.msu.cme.pyro.cluster.upgma.UPGMAClusterEdges;
import edu.msu.cme.pyro.cluster.upgma.UPGMAEdgeReader;
import edu.msu.cme.pyro.cluster.utils.Cluster;
import edu.msu.cme.pyro.cluster.utils.ClusterFactory;
import edu.msu.cme.pyro.cluster.io.ClusterFileOutput;
import edu.msu.cme.pyro.cluster.io.ClusterOutput;
import edu.msu.cme.pyro.cluster.io.LocalEdgeReader;
import edu.msu.cme.pyro.cluster.upgma.UPGMAClusterFactory;
import edu.msu.cme.pyro.cluster.io.EdgeReader;
import edu.msu.cme.pyro.cluster.utils.AbstractClusterFactory;
import edu.msu.cme.pyro.derep.IdMapping;
import edu.msu.cme.pyro.derep.SampleMapping;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

/**
 *
 * @author fishjord
 */
public class Clustering {

    private static final Options options = new Options();

    static {
        options.addOption("m", "method", true, "Clustering method to use (default=complete, others={upgma,single})");
        options.addOption("i", "id-mapping", true, "Id mapping file");
        options.addOption("s", "sample-mapping", true, "Sample mapping file");
        options.addOption("p", "psi", true, "Psi value (optional)");
        options.addOption("S", "step", true, "Step value (default=.01 [1%])");
        options.addOption("d", "dist-file", true, "Sorted column distance matrix file");
        options.addOption("o", "outfile", true, "Output file");
        options.addOption("t", "tree-file", true, "Write out merges to file");
        options.addOption("C", "no-clust-out", false, "Don't write out clustering");
    }

    public static void doUPGMA(UPGMAClusterFactory fact, UPGMAEdgeReader reader, int psi, double step, ClusterOutput clustOut) throws IOException {
        int realStep = (int) Math.round(step * DistanceCalculator.MULTIPLIER);
        int cutoff = 0;
        int lastCutoff = cutoff;
        UPGMAClusterEdges edgeHolder = fact.getEdgeHolder();
        ThickEdge ubEdge;
        long avgTimeMerging = 0;
        long avgTimeSpentValidating = 0;

        //System.out.println("Step=" + step + " realStep=" + realStep);

        PrintStream out = new PrintStream(new FileOutputStream("timing.txt", true));
        long startTime = System.currentTimeMillis();
        try {
            while (reader.loadMoreEdges()) {
                int lambda = reader.getUnknownLambda();
                edgeHolder.setLambda(lambda);

                long t = System.currentTimeMillis();
                System.out.println("lambda=" + lambda);
                while ((ubEdge = edgeHolder.getLowestLB()) != null) {
                    //edgeHolder.lbHeap.rebuild();
                    //edgeHolder.ubHeap.rebuild();
                    //System.out.println("Lowest upperbound=" + ubEdge.getBound(psi));
                    if (ubEdge.getBound(psi) <= edgeHolder.getLowestLB().getBound(lambda)) {
                    } else if (edgeHolder.getSecondLowestLB() != null && ubEdge.getBound(psi) <= edgeHolder.getSecondLowestLB().getBound(lambda)) {
                    } else {
                        ThickEdge lowestLB = edgeHolder.getLowestLB();
                        ThickEdge secondLowestLB = edgeHolder.getSecondLowestLB();
                        System.out.println("Merging halted, lambda=" + lambda + " ub=" + ubEdge.getBound(psi) + ", lb= " + lowestLB.getBound(lambda) + " second_lb=" + secondLowestLB.getBound(lambda));
                        System.out.println("ubEdge edge ci=" + ubEdge.getCi().getId() + ", " + ubEdge.getCi().getNumberOfSeqs() + " cj=" + ubEdge.getCj().getId() + ", " + ubEdge.getCj().getNumberOfSeqs() + " tk=" + ubEdge.getSeenEdges() + ", " + ubEdge.getSeenDistances() + ", " + ubEdge.getBound(lambda) + ", " + ubEdge.getBound(psi));
                        System.out.println("lowestLB edge ci=" + lowestLB.getCi().getId() + ", " + lowestLB.getCi().getNumberOfSeqs() + " cj=" + lowestLB.getCj().getId() + ", " + lowestLB.getCj().getNumberOfSeqs() + " tk=" + lowestLB.getSeenEdges() + ", " + lowestLB.getSeenDistances() + ", " + lowestLB.getBound(lambda) + ", " + lowestLB.getBound(psi));
                        System.out.println("secondLowestLB edge ci=" + secondLowestLB.getCi().getId() + ", " + secondLowestLB.getCi().getNumberOfSeqs() + " cj=" + secondLowestLB.getCj().getId() + ", " + secondLowestLB.getCj().getNumberOfSeqs() + " tk=" + secondLowestLB.getSeenEdges() + ", " + secondLowestLB.getSeenDistances() + ", " + secondLowestLB.getBound(lambda) + ", " + secondLowestLB.getBound(psi));

                        break;
                    }

                    /*if (edge.getMinLBEdge() != null && edge.getMinLBEdge().getBound(lambda) < edge.getMinDistEdge().getBound(psi) &&
                    edge.getNextLBEdge() != null && edge.getNextLBEdge().getBound(lambda) < edge.getMinDistEdge().getBound(psi)) {
                    break;
                    }*/

                    if (ubEdge.getBound(psi) > cutoff) {
                        clustOut.printClusters(fact, cutoff);
                        lastCutoff = cutoff;
                        while (ubEdge.getBound(psi) > cutoff) {
                            cutoff += realStep;
                        }
                    }

                    fact.mergeCluster(ubEdge.getCi(), ubEdge.getCj(), ubEdge.getBound(psi));

                }

                avgTimeMerging = (avgTimeMerging + (System.currentTimeMillis() - t)) / 2;

                t = System.currentTimeMillis();
                if (!edgeHolder.lbHeap.validate(System.err)) {
                    throw new IllegalStateException("LBHeap validation failed");
                }
                if (!edgeHolder.ubHeap.validate(System.err)) {
                    throw new IllegalStateException("UBHeap validation failed");
                }
                avgTimeSpentValidating = (avgTimeSpentValidating + (System.currentTimeMillis() - t)) / 2;
            }
            clustOut.printClusters(fact, cutoff);
        } catch (CannotLoadMoreEdgesException e) {
            System.err.println("Clustering halted at or slightly before cutoff " + ((float) lastCutoff / DistanceCalculator.MULTIPLIER) + " more edges required to cluster further");
        }

        out.println("Total cluster time\t" + (System.currentTimeMillis() - startTime));
        out.println("Time spent io\t" + reader.timeSpendReadingDists);
        out.println("Average time merging\t" + avgTimeMerging);
        out.println("Average time validating\t" + avgTimeSpentValidating);
        out.println("Time spent rebuilding lb heap\t" + edgeHolder.lbHeap.timeSpentRebuilding);
        out.println("Time spent rebuilding ub heap\t" + edgeHolder.ubHeap.timeSpentRebuilding);
        out.close();
    }

    public static void doCompleteLinkage(ClusterFactory fact, EdgeReader reader, double step, ClusterOutput clustOut) throws IOException {
        int realStep = (int) Math.round(step * DistanceCalculator.MULTIPLIER);
        System.out.println("Doing complete linkage clustering with step " + step + " (realstep=" + realStep + ")");
        int cutoff = 0;
        ThinEdge edge;

        while ((edge = reader.nextThinEdge()) != null) {
            if (edge.getDist() > cutoff) {
                clustOut.printClusters(fact, cutoff);
                while (edge.getDist() > cutoff) {
                    cutoff += realStep;
                }
            }

            Cluster ci = fact.getCluster(edge.getSeqi());
            Cluster cj = fact.getCluster(edge.getSeqj());

            if (ci == null) {
                ci = fact.createSingleton(edge.getSeqi());
            }
            if (cj == null) {
                cj = fact.createSingleton(edge.getSeqj());
            }

            ThickEdge tkEdge = fact.getThickEdge(ci, cj);
            if (tkEdge == null) {
                tkEdge = fact.createThickEdge(ci, cj, edge);
            } else {
                tkEdge.addEdge(edge.getDist());
            }

            if (ci.getNumberOfSeqs() * cj.getNumberOfSeqs() <= tkEdge.getSeenEdges()) {
                fact.mergeCluster(ci, cj, edge.getDist());
            }
        }
        clustOut.printClusters(fact, cutoff);
        reader.close();
    }

    public static void doSingleLinkage(ClusterFactory fact, EdgeReader reader, double step, ClusterOutput clustOut) throws IOException {
        int realStep = (int) Math.round(step * DistanceCalculator.MULTIPLIER);
        int cutoff = 0;
        ThinEdge edge;

        while ((edge = reader.nextThinEdge()) != null) {

            Cluster ci = fact.getCluster(edge.getSeqi());
            Cluster cj = fact.getCluster(edge.getSeqj());

            if (ci == null) {
                ci = fact.createSingleton(edge.getSeqi());
            }
            if (cj == null) {
                cj = fact.createSingleton(edge.getSeqj());
            }

            if (ci != cj) {

                if (edge.getDist() > cutoff) {
                    clustOut.printClusters(fact, cutoff);
                    while (edge.getDist() > cutoff) {
                        cutoff += realStep;
                    }
                }

                fact.mergeCluster(ci, cj, edge.getDist());
            }
        }
        clustOut.printClusters(fact, cutoff);
        reader.close();
    }

    public static void main(String[] args) throws Exception {
        String method = "complete";
        File outFile;

        File distFile;

        double step = .01f;
        double psi = -1;
        int numSeqs = -1;
        File mergesFile = null;
        ClusterOutput clustOut;

        CommandLineParser parser = new PosixParser();

        try {
            CommandLine line = parser.parse(options, args);

            if (line.hasOption("method")) {
                method = line.getOptionValue("method");
            }

            if (line.hasOption("tree-file")) {
                mergesFile = new File(line.getOptionValue("tree-file"));
            }

            if (!line.hasOption("no-clust-out")) {
                IdMapping<Integer> idMapping;
                SampleMapping<String> sampleMapping;

                if (line.hasOption("id-mapping")) {
                    idMapping = IdMapping.fromFile(new File(line.getOptionValue("id-mapping")));
                } else {
                    throw new Exception("Id mapping file is required");
                }

                if (line.hasOption("sample-mapping")) {
                    sampleMapping = SampleMapping.fromFile(new File(line.getOptionValue("sample-mapping")));
                } else {
                    throw new Exception("Sample mapping file is required");
                }

                if (line.hasOption("outfile")) {
                    outFile = new File(line.getOptionValue("outfile"));
                } else {
                    throw new Exception("Output file is required");
                }

                if (line.hasOption("step")) {
                    step = new Double(line.getOptionValue("step"));
                }

                numSeqs = idMapping.size();
                clustOut = new ClusterFileOutput(idMapping, sampleMapping, new PrintStream(outFile));
            } else {
                if (mergesFile == null) {
                    throw new Exception("Merges output file is required if not outputing clustering");
                }

                clustOut = new ClusterOutput() {

                    public void printClusters(AbstractClusterFactory factory, int step) {
                    }

                    public void close() {
                    }
                };

                step = 1;
            }

            if (line.hasOption("dist-file")) {
                distFile = new File(line.getOptionValue("dist-file"));
            } else {
                throw new Exception("Distance matrix is required");
            }

            if (line.hasOption("psi")) {
                if (!method.equals("upgma")) {
                    throw new Exception("Psi can only be specified with upgma clustering");
                }
                psi = new Double(line.getOptionValue("psi"));
            }

        } catch (Exception e) {
            new HelpFormatter().printHelp("Clustering", options, true);
            System.out.println("Error: " + e.getMessage());
            return;
        }

        if (method.equals("complete")) {
            long startTime = System.currentTimeMillis();
            ClusterFactory f = null;

            if (mergesFile != null) {
                f = new ClusterFactory(mergesFile);
            } else {
                f = new ClusterFactory();
            }

            doCompleteLinkage(f, new LocalEdgeReader(distFile), step, clustOut);
            f.finish();
            System.out.println("Clustering complete: " + (System.currentTimeMillis() - startTime));
        } else if (method.equals("single")) {
            long startTime = System.currentTimeMillis();
            ClusterFactory f = null;

            if (mergesFile != null) {
                f = new ClusterFactory(mergesFile);
            } else {
                f = new ClusterFactory();
            }
            doSingleLinkage(f, new LocalEdgeReader(distFile), step, clustOut);
            f.finish();
            System.out.println("Clustering complete: " + (System.currentTimeMillis() - startTime));
        } else if (method.equals("upgma")) {
            UPGMAEdgeReader reader;

            int intPsi;
            if (psi < 0) {
                intPsi = UPGMAEdgeReader.getPsiFromFile(distFile);
            } else {
                intPsi = (int) (psi * DistanceCalculator.MULTIPLIER);
            }

            UPGMAClusterFactory factory = null;

            if (mergesFile != null) {
                factory = new UPGMAClusterFactory(intPsi, mergesFile);
            } else {
                factory = new UPGMAClusterFactory(intPsi);
            }
            reader = new UPGMAEdgeReader(intPsi, distFile, factory, numSeqs);

            long startTime = System.currentTimeMillis();
            doUPGMA(factory, reader, intPsi, step, clustOut);
            factory.finish();
            System.out.println("Clustering complete: " + ((System.currentTimeMillis() - startTime) - reader.getTimeSpentLookingahead()));
            System.out.println("Lookaheads performed: " + reader.getLookaheads());
            System.out.println("Time spent Looking ahead: " + reader.getTimeSpentLookingahead());
        } else {
            System.out.println("Valid methods are {single, average, complete}");
        }

        if(clustOut != null) {
            clustOut.close();
        }
    }
}
