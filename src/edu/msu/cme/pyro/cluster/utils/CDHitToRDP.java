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

import edu.msu.cme.pyro.cluster.dist.DistanceCalculator;
import edu.msu.cme.pyro.cluster.io.ClusterFileOutput;
import edu.msu.cme.pyro.cluster.io.ClusterOutput;
import edu.msu.cme.pyro.derep.IdMapping;
import edu.msu.cme.pyro.derep.SampleMapping;
import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author fishjord
 */
public class CDHitToRDP {

    public static class EasyClustFactory implements AbstractClusterFactory {

        private Map<Integer, Cluster> clustMap = new HashMap();
        private Map<Cluster, Set<Integer>> clustersToSeqs = new HashMap();

        public void addCluster(int clustid, Set<Integer> seqids) {
            Cluster c = new Cluster(clustid, seqids.size());
            for (Integer i : seqids) {
                clustMap.put(i, c);
            }

            clustersToSeqs.put(c, seqids);
        }

        public Cluster getCluster(int seqid) {
            return clustMap.get(seqid);
        }

        public Set<Integer> getSeqsInCluster(Cluster c) {
            return clustersToSeqs.get(c);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4 && args.length != 5) {
            System.err.println("USAGE: ClusterReplay <idmapping> <sample_mapping> <cdhit_file> <cluster_out>");
            return;
        }

        IdMapping<Integer> idMapping = IdMapping.fromFile(new File(args[0]));
        SampleMapping<String> sampleMapping = SampleMapping.fromFile(new File(args[1]));

        Map<String, Integer> reverseIdMapping = idMapping.getReverseMapping();

        ClusterOutput clustOut = new ClusterFileOutput(idMapping, sampleMapping, new PrintStream(new File(args[3])));
        EasyClustFactory factory = new EasyClustFactory();
        BufferedReader cdhitReader = new BufferedReader(new FileReader(args[2]));

        double cutoff = 100, dist;

        System.err.println("Beginning CDHit cluster file conversion");
        System.err.println("Input file: " + args[2]);
        System.err.println("Output file: " + args[3]);

        Set<Integer> seqs = null;
        int clusterId = -1;

        String line = null;

        int lineno = 0;
        Integer intSeqid;
        String strSeqid;

        //>Cluster 0
        //0       402nt, >GE87P2X01C4TG4_cs_nbp_rc... at +/99.75%
        try {
            while ((line = cdhitReader.readLine()) != null) {
                lineno++;
                if (line.startsWith(">Cluster")) {
                    if (seqs != null) {
                        factory.addCluster(clusterId, seqs);
                    }

                    seqs = new HashSet();
                    clusterId = Integer.valueOf(line.split("\\s+")[1]);
                    System.err.println("Processing cluster " + clusterId);
                    continue;
                }

                String[] lexemes = line.split("\\s+");
                if (lexemes.length != 5 && lexemes.length != 4) {
                    System.err.println("Skipping line " + line);
                    continue;
                }

                /*
                 * Some parsing checks, look for the at, first thing is a
                 * number, etc
                 */
                Integer.valueOf(lexemes[0]); //Make sure the first item is a number
                if ((lexemes.length == 5 && !lexemes[3].equals("at")) || (lexemes.length == 4 && !lexemes[3].equals("*"))) {
                    throw new IOException("Malformed cluster member line (missing 'at')");
                }

                if (!lexemes[2].startsWith(">") || !lexemes[2].endsWith("...")) {
                    throw new IOException("Malformed seqid token");
                }

                if (lexemes.length == 5 && lexemes[4].endsWith("%")) {
                    dist = Double.valueOf(lexemes[4].substring(2, lexemes[4].length() - 1));
                    if (dist < cutoff) {
                        cutoff = dist;
                    }
                } else if (lexemes.length == 5) {
                    throw new IOException("Malformed percent identity token");
                }

                strSeqid = lexemes[2].substring(1, lexemes[2].length() - 3);
                intSeqid = reverseIdMapping.get(strSeqid);

                if (intSeqid == null) {
                    throw new IOException("Couldn't find id " + strSeqid + " in the id mapping, perhaps cdhit truncated the description line?");
                }

                seqs.add(intSeqid);
            }

        } catch (Exception e) {
            throw new IOException("Failed while parsing line " + lineno + ": " + line, e);
        }
        
        if (seqs != null) {
            factory.addCluster(clusterId, seqs);
        }

        System.out.println((100 - cutoff) + ": " + clusterId + " clusters");

        clustOut.printClusters(factory, (int) ((1 - cutoff / 100) * DistanceCalculator.MULTIPLIER));
        clustOut.close();

        cdhitReader.close();
    }
}
