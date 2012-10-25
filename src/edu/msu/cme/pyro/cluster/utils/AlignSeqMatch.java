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

import edu.msu.cme.pyro.derep.IdMapping;
import edu.msu.cme.rdp.alignment.pairwise.rna.DistanceModel;
import edu.msu.cme.rdp.alignment.pairwise.rna.IdentityDistanceModel;
import edu.msu.cme.rdp.alignment.pairwise.rna.OverlapCheckFailedException;
import edu.msu.cme.rdp.readseq.readers.SequenceReader;
import edu.msu.cme.rdp.readseq.readers.SeqReader;
import edu.msu.cme.rdp.readseq.readers.Sequence;
import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author fishjord
 */
public class AlignSeqMatch {

    private Map<Sequence, byte[]> seeds;
    private DistanceModel model = null;
    private float minOverlapRatio = .5f;

    public static class AlignSeqMatchResult {

        private Set<Sequence> results;
        private double dist;

        public AlignSeqMatchResult(Set<Sequence> results, double dist) {
            this.results = Collections.unmodifiableSet(results);
            this.dist = dist;
        }

        public double getDist() {
            return dist;
        }

        public Set<Sequence> getResults() {
            return results;
        }
    }

    public AlignSeqMatch(Collection<Sequence> seedSeqs, boolean metric) {
        this.seeds = new HashMap();

        for (Sequence seq : seedSeqs) {
            this.seeds.put(seq, seq.getSeqString().getBytes());
        }

        this.seeds = Collections.unmodifiableMap(this.seeds);
        this.model = new IdentityDistanceModel(metric);
    }

    public AlignSeqMatchResult match(Sequence querySeq) throws OverlapCheckFailedException {
        Set<Sequence> ret = new HashSet();
        byte[] query = querySeq.getSeqString().getBytes();
        int minOverlap = (int) (query.length * minOverlapRatio);

        double bestDist = Double.MAX_VALUE;
        for (Sequence seedSeq : seeds.keySet()) {
            byte[] seed = seeds.get(seedSeq);

            try {
                double dist = model.getDistance(seed, query, minOverlap);
                if (bestDist > dist) {
                    ret.clear();
                    ret.add(seedSeq);
                    bestDist = dist;
                } else if (bestDist == dist) {
                    ret.add(seedSeq);
                }
            } catch (OverlapCheckFailedException e) {
            }
        }

        return new AlignSeqMatchResult(ret, bestDist);
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3 && args.length != 4) {
            System.err.println("USAGE: <idmapping> <seed_seqs> <query_seqs> [metric?=true]");
            return;
        }

        File idMappingFile = new File(args[0]);
        File seedFile = new File(args[1]);
        File queryFile = new File(args[2]);

        boolean metric = true;
        if (args.length == 4) {
            metric = Boolean.parseBoolean(args[3]);
        }

        SeqReader reader = new SequenceReader(queryFile);
        AlignSeqMatch match = new AlignSeqMatch(SequenceReader.readFully(seedFile), metric);
        IdMapping<Integer> idMapping = IdMapping.fromFile(idMappingFile);
        Sequence query;

        //System.out.println("#queryId\tseedId\tdistance");
        Map<Sequence, Integer> hitMap = new HashMap();

        while ((query = reader.readNextSequence()) != null) {
            AlignSeqMatchResult result = match.match(query);

            for (String qid : idMapping.getIds(idMapping.getReverseMapping().get(query.getSeqName()))) {
                for (Sequence seed : result.getResults()) {
                    System.out.println(qid + "\t" + seed.getSeqName() + "\t" + result.getDist());

                    if (!hitMap.containsKey(seed)) {
                        hitMap.put(seed, 0);
                    }

                    hitMap.put(seed, hitMap.get(seed) + 1);
                }
            }
        }

        /*System.out.println("\n#seedId\tcount");
        for(Sequence seed : hitMap.keySet()) {
        System.out.println(seed.getSeqName() + "\t" + hitMap.get(seed));
        }*/
    }
}
