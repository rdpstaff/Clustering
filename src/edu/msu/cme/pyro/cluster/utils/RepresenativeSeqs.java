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
package edu.msu.cme.pyro.cluster.utils;

import edu.msu.cme.pyro.cluster.io.RDPClustParser;
import edu.msu.cme.pyro.cluster.io.RDPClustParser.ClusterSample;
import edu.msu.cme.pyro.cluster.io.RDPClustParser.Cutoff;
import edu.msu.cme.pyro.derep.IdMapping;
import edu.msu.cme.rdp.alignment.pairwise.rna.DistanceModel;
import edu.msu.cme.rdp.alignment.pairwise.rna.IdentityDistanceModel;
import edu.msu.cme.rdp.alignment.pairwise.rna.OverlapCheckFailedException;
import edu.msu.cme.rdp.readseq.readers.Sequence;
import edu.msu.cme.rdp.readseq.readers.IndexedSeqReader;
import edu.msu.cme.rdp.readseq.utils.SeqUtils;
import edu.msu.cme.rdp.readseq.writers.FastaWriter;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

/**
 *
 * @author fishjord
 */
public class RepresenativeSeqs {

    private static final Options options = new Options();
    private static final DecimalFormat f = new DecimalFormat("####0.#####");
    
    static {
        options.addOption("o", "out", true, "Output directory (default=.)");
        options.addOption("m", "mask-seq", true, "Mask sequence id");
        options.addOption("p", "pref-seqids", true, "Preferential sequence id file");
        options.addOption("l", "longest", false, "Select longest sequences instead of least dist squared");
        options.addOption("i", "invert", false, "Invert preferential sequence id (give preference to seqids not in the file)");
        options.addOption("I", "id-mapping", true, "Id mapping used when clustering [required if using dereplicated seq file]");
        options.addOption("s", "one-rep-per-otu", false, "One representative sequence for each OTU. Default is false. The default"
                + " returns one rep seq for each sample at each OTU");
        options.addOption("c", "use-cluster-id", false, "Use the cluster id as the seq id.");
    }


    public static Map<String, List<Sequence>> getRepresenativeSeqs(RDPClustParser parser, String cutoffStr, IndexedSeqReader seqReader, String maskId, IdMapping<Integer> idMapping, Set<String> prefSeqids, boolean invert, DistanceModel m, int overlapMin, boolean useClusterID) throws OverlapCheckFailedException, IOException {
        Map<String, List<Sequence>> ret = new HashMap();
        for (ClusterSample sample : parser.getClusterSamples()) {
            ret.put(sample.getName(), new ArrayList());
        }

        Cutoff cutoff = parser.getCutoff(cutoffStr);

        int preferedSeqs = 0;
        int nonPreferedSeqs = 0;

        char[] maskSeq = null;
        if (maskId != null) {
            maskSeq = seqReader.readSeq(maskId).getSeqString().toCharArray();
        }

        Map<String, String> idToExemplar = new HashMap();
        if (idMapping != null) {
            for (int i : idMapping.getAll()) {
                String exId = idMapping.getIds(i).get(0);

                for (String id : idMapping.getIds(i)) {
                    idToExemplar.put(id, exId);
                }
            }
        }

        for (String sample : cutoff.getClusters().keySet()) {
            for (Cluster clust : cutoff.getClusters().get(sample)) {
                Set<String> seqsInClust = new LinkedHashSet();
                List<String> realSeqids = new ArrayList();

                for (String seqid : cutoff.getClustersToSeqs().get(clust)) {
                    realSeqids.add(seqid);
                    String exid = idToExemplar.get(seqid);
                    if (exid == null) {
                        exid = seqid;
                    }

                    seqsInClust.add(exid);
                }

                if (seqsInClust.isEmpty()) {
                    continue;
                }

                List<Sequence> seqs = seqReader.readSeqs(seqsInClust);

                Sequence repSeq = getRep(seqs, clust.getId(), maskSeq, realSeqids, prefSeqids, invert, m, overlapMin, useClusterID );
                ret.get(sample).add(repSeq);
                if (!prefSeqids.contains(repSeq.getSeqName())) {
                    nonPreferedSeqs++;
                } else {
                    preferedSeqs++;
                }

            }
        }

        return ret;
    }

    public static void printOneRepresenativeSeqPerOTU(RDPClustParser parser, String cutoffStr, IndexedSeqReader seqReader, String maskId, IdMapping<Integer> idMapping, Set<String> prefSeqids, boolean invert, DistanceModel m, int overlapMin, FastaWriter out, boolean useClusterID) throws OverlapCheckFailedException, IOException {
        Cutoff cutoff = parser.getCutoff(cutoffStr);

        int preferedSeqs = 0;
        int nonPreferedSeqs = 0;

        char[] maskSeq = null;
        if (maskId != null) {
            maskSeq = seqReader.readSeq(maskId).getSeqString().toCharArray();
        }

        Map<String, String> idToExemplar = new HashMap();
        if (idMapping != null) {
            for (int i : idMapping.getAll()) {
                String exId = idMapping.getIds(i).get(0);

                for (String id : idMapping.getIds(i)) {
                    idToExemplar.put(id, exId);
                }
            }
        }

        // find the clusters from all the samples in the same otu
        HashMap<Integer, HashSet<Cluster>> clusterMap = new HashMap<Integer, HashSet<Cluster>>(); // clusterID, cluster
        for (String sample : cutoff.getClusters().keySet()) {

            for (Cluster clust: cutoff.getClusters().get(sample)){
                int clusterID = clust.getId();
                HashSet<Cluster> clusterSet = clusterMap.get(clusterID);

                if ( clusterSet == null){
                    clusterSet = new HashSet<Cluster>();
                    clusterMap.put(clusterID, clusterSet);
                }
                clusterSet.add(clust);
            }
        }


        for (Integer clusterID : clusterMap.keySet() ) {
            Set<String> seqsInClust = new LinkedHashSet();

            int totalNumberOfSeqs = 0;

            for (Cluster clust : clusterMap.get(clusterID)) {
                totalNumberOfSeqs += clust.getNumberOfSeqs();
                for (String seqid : cutoff.getClustersToSeqs().get(clust)) {
                    String exid = idToExemplar.get(seqid);
                    if (exid == null) {
                        exid = seqid;
                    }

                    seqsInClust.add(exid);
                }
            }

            if (seqsInClust.isEmpty()) {
                continue;
            }

            List<Sequence> seqs = seqReader.readSeqs(seqsInClust);

            Sequence repSeq = getRep(seqs, clusterID, maskSeq, new ArrayList(seqsInClust), prefSeqids, invert, m, overlapMin, useClusterID );
	    out.writeSeq(repSeq);
            if (!prefSeqids.contains(repSeq.getSeqName())) {
                nonPreferedSeqs++;
            } else {
                preferedSeqs++;
            }
        }

    }

    private static Sequence getRep(List<Sequence> seqs, int clusterID, char[] maskSeq, List<String> realSeqids, Set<String> prefSeqids, boolean invert, DistanceModel m, int overlapMin, boolean useClusterID) throws OverlapCheckFailedException, IOException{

        int bestSeq = -1;
        int bestPrefSeq = -1;
        double bestScore = Double.MAX_VALUE;
        double bestPrefScore = Double.MAX_VALUE;
        double maxDist = 0;
        if (m != null) {
            
            double[][] subMatrix = new double[seqs.size()][seqs.size()];

            for (int s1 = 0; s1 < subMatrix.length; s1++) {
                subMatrix[s1][s1] = 0;
                byte[] seqStr1;

                if (maskSeq != null) {
                    seqStr1 = SeqUtils.getMaskedSeq(seqs.get(s1).getSeqString(), maskSeq).getBytes();
                } else {
                    seqStr1 = seqs.get(s1).getSeqString().getBytes();
                }

                for (int s2 = s1 + 1; s2 < subMatrix.length; s2++) {
                    String seqStr2 = seqs.get(s2).getSeqString();
                    if (maskSeq != null) {
                        seqStr2 = SeqUtils.getMaskedSeq(seqStr2, maskSeq);
                    }

                    subMatrix[s1][s2] = subMatrix[s2][s1] = m.getDistance(seqStr1, seqStr2.getBytes(), overlapMin);
                }
            }

            for (int index = 0; index < subMatrix.length; index++) {
                double score = 0;
                for (int col = 0; col < subMatrix[index].length; col++) {
                     if (index == col) {
                        continue;
                    }
                    score += subMatrix[index][col] * subMatrix[index][col];                    
                    if (subMatrix[index][col] > maxDist) {
                        maxDist = subMatrix[index][col];
                    }
                }

                if (score < bestScore) {
                    bestSeq = index;
                    bestScore = score;
                }
                
                boolean contained = prefSeqids.contains(realSeqids.get(index));
                boolean preferred = (contained && !invert) || (!contained && invert);

                if (preferred && score < bestPrefScore) {
                    bestPrefSeq = index;
                    bestPrefScore = score;
                }
            }
        } else {
            int bestLength = 0;
            int bestPrefLength = 0;
            for (int index = 0; index < seqs.size(); index++) {
                String seq;

                if (maskSeq != null) {
                    seq = SeqUtils.getMaskedSeq(seqs.get(index).getSeqString(), maskSeq);
                } else {
                    seq = seqs.get(index).getSeqString();
                }

                int length = SeqUtils.getUnalignedSeqString(seq).length();

                if (length > bestLength) {
                    bestSeq = index;
                    bestLength = length;
                }

                boolean contained = prefSeqids.contains(realSeqids.get(index));
                boolean preferred = (contained && !invert) || (!contained && invert);

                if (preferred && length > bestPrefLength) {
                    bestPrefSeq = index;
                    bestPrefLength = length;
                }
            }
        }

        // if we choose the longest seq from an OTU, these values are not available
        String maxDistStr = "NA";
        if ( m != null){
            maxDistStr = f.format(maxDist);
        }
        String minSumSquareStr = "NA";
        if ( m != null){
            minSumSquareStr = f.format(bestScore);
        }
        
        if (bestPrefSeq == -1) {
            if(useClusterID){
                return new Sequence("cluster_" + clusterID, "seq_id=" + realSeqids.get(bestSeq) + ",prefered=false,cluster=" + clusterID + ",clustsize=" + seqs.size() + ",maxDist=" + maxDistStr + ",minSumSquare=" + minSumSquareStr, seqs.get(bestSeq).getSeqString());
            } else {
                return new Sequence(realSeqids.get(bestSeq), "prefered=false,cluster=" + clusterID + ",clustsize=" + seqs.size() + ",maxDist=" + maxDistStr + ",minSumSquare=" + minSumSquareStr, seqs.get(bestSeq).getSeqString());
            }
        } else {
            if(useClusterID){
                return new Sequence("cluster_" + clusterID, "seq_id=" + realSeqids.get(bestPrefSeq) + ",prefered=true,cluster=" + clusterID + ",clustsize=" + seqs.size(), seqs.get(bestPrefSeq).getSeqString());
            } else{
                return new Sequence(realSeqids.get(bestPrefSeq), "prefered=true,cluster=" + clusterID + ",clustsize=" + seqs.size(), seqs.get(bestPrefSeq).getSeqString());

            }
        }


    }


    public static void main(String[] args) throws Exception {
        String maskSeqId = null;
        File outputDir = new File(".");
        Set<String> prefSeqids = new HashSet();
        boolean invert = false;
        IdMapping<Integer> idMapping = null;
        DistanceModel model;
        boolean oneRepPerOTU = false;
        boolean useClusterID = false;

        try {
            CommandLine line = new PosixParser().parse(options, args);

            if (line.hasOption("mask-seq")) {
                maskSeqId = line.getOptionValue("mask-seq");
            }

            if (line.hasOption("out")) {
                outputDir = new File(line.getOptionValue("out"));
            }

            if (line.hasOption("id-mapping")) {
                idMapping = IdMapping.fromFile(new File(line.getOptionValue("id-mapping")));
            }

            if (line.hasOption("pref-seqids")) {
                BufferedReader reader = new BufferedReader(new FileReader(line.getOptionValue("pref-seqids")));
                String l;
                while ((l = reader.readLine()) != null) {
                    l = l.trim();
                    if (!l.equals("")) {
                        prefSeqids.add(l);
                    }
                }
            }

            if (line.hasOption("longest")) {
                model = null;
            } else {
                model = new IdentityDistanceModel(false);
            }
            if (line.hasOption("one-rep-per-otu")) {
                oneRepPerOTU = true;
            }
            if (line.hasOption("use-cluster-id")) {
                useClusterID = true;
            }

            invert = line.hasOption("invert");

            args = line.getArgs();

            if (args.length != 3) {
                throw new Exception("Unexpected number of command line arguments");
            }
        } catch (Exception e) {
            new HelpFormatter().printHelp("RepresenativeSeqs [options] <clust_file> <cutoff> <seq_file>", options);
            System.out.println("Error: " + e.getMessage());
            return;
        }

        File clustFile = new File(args[0]);
        File seqFile = new File(args[2]);

        String cutoffStr = args[1];

        RDPClustParser clustParser = new RDPClustParser(clustFile, true);
        IndexedSeqReader seqReader = new IndexedSeqReader(seqFile);

        Map<String, List<Sequence>> repSeqMap = null;
        if ( !oneRepPerOTU){
            repSeqMap = RepresenativeSeqs.getRepresenativeSeqs(clustParser, cutoffStr, seqReader, maskSeqId, idMapping, prefSeqids, invert, model, 1, useClusterID);
	    for (String sample : repSeqMap.keySet()) {
		FastaWriter writer = new FastaWriter(new File(outputDir, sample + "_representatives.fasta"));
		for (Sequence s : repSeqMap.get(sample)) {
		    writer.writeSeq(s);
		}
		writer.close();
	    }
        } else {
	    FastaWriter out = new FastaWriter(new File(outputDir, clustFile.getName() + "_rep_seqs.fasta"));
            RepresenativeSeqs.printOneRepresenativeSeqPerOTU(clustParser, cutoffStr, seqReader, maskSeqId, idMapping, prefSeqids, invert, model, 1, out, useClusterID);
	    out.close();
        }
        clustParser.close();
    }
}
