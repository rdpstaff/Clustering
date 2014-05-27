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
package edu.msu.cme.pyro.cluster.io;

import edu.msu.cme.pyro.cluster.dist.DistanceCalculator;
import edu.msu.cme.pyro.cluster.utils.Cluster;
import edu.msu.cme.rdp.readseq.utils.BufferedRandomAccessFile;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Set;

/**
 *
 * @author fishjord
 */
public final class RDPClustParser {

    public static class ClusterSample {

        private String name;
        private int seqs;

        public ClusterSample(String name, int seqs) {
            this.name = name;
            this.seqs = seqs;
        }

        public String getName() {
            return name;
        }

        public int getSeqs() {
            return seqs;
        }
    }

    public static class Cutoff {

        private String cutoff;
        private int numClusters;
        private Map<String, List<Cluster>> clusters;
        private Map<Cluster, Set<String>> clustersToSeqs;

        public Cutoff(String cutoff, int numClusters, Map<String, List<Cluster>> clusters, Map<Cluster, Set<String>> clustersToSeqs) {
            this.cutoff = cutoff;
            this.numClusters = numClusters;
            this.clusters = clusters;
            this.clustersToSeqs = clustersToSeqs;
        }

        public Map<String, List<Cluster>> getClusters() {
            return clusters;
        }

        public Map<Cluster, Set<String>> getClustersToSeqs() {
            return clustersToSeqs;
        }

        public String getCutoff() {
            return cutoff;
        }

        public int getNumClusters() {
            return numClusters;
        }
    }
    protected List<ClusterSample> clusterSamples = new ArrayList();
    private Map<Integer, Long> cutoffPosMap = new HashMap();
    private List<Integer> cutoffs = new ArrayList();
    private Map<String, Integer> cutoffStrings = new LinkedHashMap();
    private RandomAccessFile clusterFile;
    private final boolean parseSeqids;

    public RDPClustParser(File clustFile) throws IOException {
        this(clustFile, false);
    }

    public RDPClustParser(File clustFile, boolean keepSeqs) throws IOException {
        clusterFile = new BufferedRandomAccessFile(clustFile, "r", 1024);
        this.parseSeqids = keepSeqs;

        parseClusterHeader();
    }

    private void parseClusterHeader() throws IOException {
        String fileLine = clusterFile.readLine();
        String seqCountLine = clusterFile.readLine();

        if (fileLine == null || seqCountLine == null) {
            throw new IOException("Unexpected end of file");
        }

        String[] samples = fileLine.split("\\s+");
        String[] counts = seqCountLine.split("\\s+");

        if (samples[0].equals("File(s):") && samples.length != 1) {
            samples = Arrays.copyOfRange(samples, 1, samples.length);
        } else {
            throw new IOException("Malformed files line");
        }

        if (counts[0].equals("Sequences:") && counts.length != 1) {
            counts = Arrays.copyOfRange(counts, 1, counts.length);
        } else {
            throw new IOException("Malformed sequence count line line");
        }

        if (samples.length != counts.length) {
            throw new IOException("Invalid header: Different number of files and sequence counts");
        }

        for (int index = 0; index < samples.length; index++) {
            int seqs = 0;

            try {
                seqs = Integer.valueOf(counts[index]);
            } catch (NumberFormatException e) {
                throw new IOException("Sequence count " + index + "(" + counts[index] + ") is not a number");
            }

            clusterSamples.add(new ClusterSample(samples[index], seqs));
        }

        String line;
        long lastPos = clusterFile.getFilePointer();
        long returnTo = -1;

        while ((line = clusterFile.readLine()) != null) {
            if (line.startsWith("distance")) {
                String[] lexemes = line.split("\\s+");
                if (lexemes.length != 3) {
                    throw new IOException("Malformed distance line: " + line);
                }

                try {
                    double distance = Double.parseDouble(lexemes[2]);
                    int dist = cutoffDoubleToInt(distance);

                    cutoffPosMap.put(dist, lastPos);
                    cutoffs.add(dist);
                    cutoffStrings.put(lexemes[2], dist);
                } catch (NumberFormatException e) {
                    throw new IOException("Malformed distance line: " + line);
                }

                if (returnTo == -1) {
                    returnTo = lastPos;
                }
            }
            lastPos = clusterFile.getFilePointer();
        }

        if (returnTo != -1) {
            clusterFile.seek(returnTo);
        }

    }

    public void close() throws IOException {
        clusterFile.close();
    }

    public Set<String> getCutoffs() {
        return cutoffStrings.keySet();
    }

    private int cutoffStringToInt(String cutoff) {
        double dist;
        try {
            dist = Double.valueOf(cutoff);
        } catch (NumberFormatException e) {
            return 0;
        }

        return cutoffDoubleToInt(dist);
    }

    private int cutoffDoubleToInt(double dist) {
        dist = Math.round(dist * 1000);
        return (int) (dist * DistanceCalculator.MULTIPLIER);
    }

    public List<ClusterSample> getClusterSamples() {
        return clusterSamples;
    }

    /**
     * If toFind isn't a number, null is returned
     *
     * @param toFind
     * @return
     */
    public Cutoff getCutoff(String toFind) throws IOException {
        try {
            return getCutoff(Double.valueOf(toFind));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * So this method ISN'T designed for efficentcy, it will create a list of
     * all cutoffs, sort it, and then convert each cutoff to a float looking for
     * the closest match not higher than the cutoff
     *
     * Which brings us to IF THE CUTOFF ISN'T IN THE MAP the lowest cutoff <=
     * toFind is returned, or the smallest cutoff period
     *
     *
     *
     *
     *

     *
     * @param toFind
     * @return
     */
    public Cutoff getCutoff(double dToFind) throws IOException {

        if (cutoffs.isEmpty()) {
            return null;
        }

        int toFind = cutoffDoubleToInt(dToFind);

        int bestMatch = cutoffs.get(0);

        for (Integer cutoff : cutoffs) {
            //Allow for a little slosh around 0
            if (cutoff <= toFind) {
                bestMatch = cutoff;
            }
        }


        return readCutoff(bestMatch);
    }

    public boolean containsAtLeastOne(double dFrom, double dTo) {
        int from = cutoffDoubleToInt(dFrom);
        int to = cutoffDoubleToInt(dTo);
        
        for (Integer cutoff : cutoffs) {

            if (cutoff >= from && cutoff <= to) {
                return true;
            }
        }

        return false;
    }

    public List<Cutoff> getCutoffs(double dFrom, double dTo) throws IOException {
        int from = cutoffDoubleToInt(dFrom);
        int to = cutoffDoubleToInt(dTo);

        List<Cutoff> ret = new ArrayList();
        for (Integer cutoff : cutoffs) {

            if (cutoff >= from && cutoff <= to) {
                ret.add(readCutoff(cutoff));
            }
        }

        return ret;
    }

    private Cutoff readCutoff(int cutoff) throws IOException {
        Long pos = cutoffPosMap.get(cutoff);
        if (pos == null) {
            return null;
        }

        clusterFile.seek(pos);
        return readNextCutoff();
    }
    boolean eol = false;

    private String readNextLexeme() throws IOException {
        StringBuilder ret = new StringBuilder();
        int c = clusterFile.read();

        while (Character.isWhitespace(c)) {
            if (c == '\n') {
                eol = true;
                return null;
            }

            c = clusterFile.read();
        }

        do {
            ret.append((char) c);
        } while (!Character.isWhitespace((c = clusterFile.read())));

        if (c == '\n' || c == '\r') {
            eol = true;
        } else {
            eol = false;
        }

        return ret.toString();
    }

    private void skipToEol() throws IOException {
        int c;
        while (true) {
            c = clusterFile.read();
            if (c == '\n') {
                eol = true;
                return;
            }
        }
    }

    public Cutoff readNextCutoff() throws IOException {
        String cutoff = null;
        int numClusters = 0;
        Map<String, List<Cluster>> clustersMap = new HashMap();
        Map<Cluster, Set<String>> clustersToSeqs = new HashMap();
        String[] lexemes = null;

        for (ClusterSample sample : clusterSamples) {
            clustersMap.put(sample.getName(), new ArrayList());
        }


        String line = null;
        // sometimes the cutoff starts with the empty line instead of distance
        do{
            line = clusterFile.readLine();
            if (line == null) {
                return null;
            }
        } while ( line.trim().equals(""));

        lexemes = line.split("\\s+");
        if (lexemes[0].equals("distance") && lexemes[1].equals("cutoff:") && lexemes.length == 3) {
            cutoff = lexemes[2];
            try {
                Double.valueOf(cutoff);
            } catch (NumberFormatException e) {
                throw new IOException("Cutoff isn't a floating point number " + cutoff);
            }
        } else {
            throw new IOException("Malformed cluster file \"" + line + "\"");
        }

        line = clusterFile.readLine();
        if (line == null) {
            throw new IOException("Malformed cluster file");
        }

        lexemes = line.split("\\s+");

        if (lexemes[0].equals("Total") && lexemes[1].equals("Clusters:") && lexemes.length == 3) {
            try {
                numClusters = Integer.valueOf(lexemes[2]);
            } catch (NumberFormatException e) {
                throw new IOException("Malformed number of clusters for cutoff " + cutoff);
            }
        } else {
            throw new IOException("Malformed cluster file");
        }

        String clustLabel = null, sampleName = null, seqCountStr = null;
        boolean reuseLastCluster = false;
        for (int index = 0; index < numClusters; index++) {
            int clusterId = -1;
            for (int sample = 0; sample < clusterSamples.size(); sample++) {
                if (reuseLastCluster) {
                    reuseLastCluster = false;
                } else {
                    try {
                        clustLabel = readNextLexeme();
                    } catch (EOFException e) {
                        //We probably just ended up at the end of a clustering, let's just say everything is okay and run away
                        break;
                    }

                    if (clustLabel == null || clustLabel.equals("")) {
                        //We hit the end of this cutoff...
                        //Lets just check to make sure we were processing the last cluster in this cutoff...to be safe
                        if (index + 1 != numClusters) {
                            throw new IOException("Not enough clusters (" + index + " / " + numClusters + ") in cutoff " + cutoff);
                        }
                        reuseLastCluster = true;
                        break;
                    }

                    if (eol) {
                        throw new IOException("Malformed cluster line in cutoff " + cutoff + ": \"" + line + "\"");
                    }
                    sampleName = readNextLexeme();
                    if (eol) {
                        throw new IOException("Malformed cluster line in cutoff " + cutoff + ": \"" + line + "\"");
                    }
                    seqCountStr = readNextLexeme();
                    if (eol) {
                        throw new IOException("Malformed cluster line in cutoff " + cutoff + ": \"" + line + "\"");
                    }
                }

                int currClustId = 0;
                int seqCount = 0;

                try {
                    currClustId = Integer.valueOf(clustLabel);
                    seqCount = Integer.valueOf(seqCountStr);
                } catch (NumberFormatException e) {
                    throw new IOException("Invalid cluster id " + clustLabel + " or sequence count " + seqCountStr + " in cutoff " + cutoff);
                }

                if (seqCount == 0) {
                    skipToEol();
                    continue;
                }

                if (clusterId == -1) {
                    clusterId = currClustId;
                }
                if (currClustId != clusterId) {
                    //We ended up in the wrong cluster...whoopsies
                    reuseLastCluster = true;
                    break;
                    //throw new IOException("Excepected cluster id " + index + " but read " + currClustId + " in cutoff " + cutoff);
                }

                Cluster c = new Cluster(currClustId, seqCount);

                if (parseSeqids) {
                    String addSeqid;
                    Set<String> seqs = new HashSet();
                    while (!eol) {
                        addSeqid = readNextLexeme();
                        if (addSeqid != null) {
                            seqs.add(addSeqid);
                        }
                    }
                    if (seqs.size() != seqCount) {
                        System.err.println("Cutoff: " + cutoff + " ClusterLable: " + clustLabel + " sampleName: " + sampleName + " Cluster: " + currClustId + ", seqcount: " + seqCount + " read seqid: " + seqs.size());
                        System.err.println(seqs);
                        throw new IOException("Expected " + seqCount + " seqids for cluster " + index + " but counted " + seqs.size() + " instead in cutoff " + cutoff);
                    }
                    clustersToSeqs.put(c, seqs);
                } else {
                    skipToEol();
                }

                if (!clustersMap.containsKey(sampleName)) {
                    throw new IOException("Invalid sample name " + sampleName + " cluster id " + currClustId + " cutoff " + cutoff);
                }

                clustersMap.get(sampleName).add(c);
            }

            //So we basically assume that if we didn't see it, the cluster was empty
            //However, jaccard and sorensen depend on these empty samples being present
            for (int sample = 0; sample < clusterSamples.size(); sample++) {
                String missingSample = clusterSamples.get(sample).getName();

                List<Cluster> clustersInSample = clustersMap.get(missingSample);
                if (clustersInSample.size() > 0) {
                    Cluster lastCluster = clustersInSample.get(clustersInSample.size() - 1);
                    if (lastCluster.getId() != clusterId) {
                        Cluster c = new Cluster(clusterId, 0);
                        clustersInSample.add(c);
                        if (parseSeqids) {
                            clustersToSeqs.put(c, new HashSet());
                        }
                    }
                } else {
                    Cluster c = new Cluster(clusterId, 0);
                    clustersInSample.add(c);
                    if (parseSeqids) {
                        clustersToSeqs.put(c, new HashSet());
                    }
                }
            }
        }

        /*
         * We're not counting empty clusters in samples anymore (if a
         * sample/cluster doesn't appear it's assumed to be empty)
         */
        int expectedLength = -1;
        for (String missingSample : clustersMap.keySet()) {
            List<Cluster> list = clustersMap.get(missingSample);
            if (expectedLength == -1) {
                expectedLength = list.size();
            } else if (expectedLength != list.size()) {
                throw new IOException("Sample " + missingSample + " was expected to have " + expectedLength + " clusters but had " + list.size() + " at cutoff " + cutoff);
            }
        }

        return new Cutoff(cutoff, numClusters, clustersMap, clustersToSeqs);
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        /*
         * File f = new
         * File("/work/fishjord/user_files/ederson/unifrac/ederson_biofuels_clustercluster_results/ederson_biofuels_clustercluster.clust");
         * //File f = new
         * File("/work/fishjord/other_projects/titanium/titanium_04222010/processing/buk/experimental/clustering/complete.clust");
         * RDPClustParser parser = new RDPClustParser(f);
         * System.out.println("Read in cluster file in " +
         * (System.currentTimeMillis() - startTime));
         *
         * List<String> cutoffs = new ArrayList(parser.getCutoffs());
         * Collections.sort(cutoffs);
         *
         * for(String c : cutoffs) { System.out.println(c); }
         * System.out.println();
         *
         * System.out.println("Cutoff .33=" +
         * parser.getCutoff(.33).getCutoff()); System.out.println("Cutoff .05="
         * + parser.getCutoff(.05).getCutoff());
         *
         * System.out.println();
         *
         * for (Cutoff c : parser.getCutoffs(0.05, .15)) {
         * System.out.println(c.getCutoff()); } System.out.println();
         *
         * List<String> tmp = new ArrayList(Arrays.asList("0.01", "0.02",
         * "0.05", "0.09", "0.10")); String test = "0.08";
         *
         * for (String c : tmp) { System.out.println(c + " < " + test + " = " +
         * c.compareTo(test)); }
         */


        File f = new File("/scratch/wangqion/qiong_titanium/titanium_run_05272010/data_process/nifh/experimental/clustering/complete.clust");
        RDPClustParser parser = new RDPClustParser(f);
        System.out.println("Read in cluster file in " + (System.currentTimeMillis() - startTime));

        List<String> cutoffs = new ArrayList(parser.getCutoffs());
        Collections.sort(cutoffs);

        for (String c : cutoffs) {
            System.out.println(c);
        }
        System.out.println();

        System.out.println(parser.cutoffs);
    }
}
