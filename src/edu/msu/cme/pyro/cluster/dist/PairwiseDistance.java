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
package edu.msu.cme.pyro.cluster.dist;

import edu.msu.cme.pyro.derep.IdMapping;
import edu.msu.cme.rdp.alignment.AlignmentMode;
import edu.msu.cme.rdp.alignment.pairwise.PairwiseAligner;
import edu.msu.cme.rdp.alignment.pairwise.PairwiseAlignment;
import edu.msu.cme.rdp.alignment.pairwise.ScoringMatrix;
import edu.msu.cme.rdp.alignment.pairwise.rna.DistanceModel;
import edu.msu.cme.rdp.alignment.pairwise.rna.IdentityDistanceModel;
import edu.msu.cme.rdp.readseq.MaskSequenceNotFoundException;
import edu.msu.cme.rdp.readseq.SequenceType;
import edu.msu.cme.rdp.readseq.readers.IndexedSeqReader;
import edu.msu.cme.rdp.readseq.readers.Sequence;
import edu.msu.cme.rdp.readseq.utils.SeqUtils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 * @author fishjord
 */
public class PairwiseDistance {

    public static class SequenceFile {

        public List<Sequence> seqs = new ArrayList();
        SequenceType type = SequenceType.Unknown;
    }

    public static SequenceFile readSeqs(File fastaFile, String maskSeq) throws IOException, MaskSequenceNotFoundException {
        IndexedSeqReader reader = null;
        if (maskSeq == null) {
            reader = new IndexedSeqReader(fastaFile);
        } else {
            reader = new IndexedSeqReader(fastaFile, maskSeq);
        }

        SequenceFile ret = new SequenceFile();

        List<String> seqids = reader.getSeqIds();

        for (String seqid : seqids) {
            Sequence seq = reader.readSeq(seqid);
            
            if(ret.seqs.isEmpty()) {
                ret.type = SeqUtils.guessSequenceType(seq);

            }

            ret.seqs.add(seq);
        }
        reader.close();

        return ret;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("USAGE: PairwiseDistance <sequence_file> <output matrix> <id_mapping> <scoring matrix>");
            return;
        }

        File seqFile = new File(args[0]);
        File outputMatrix = new File(args[1]);
        File idMappingFile = new File(args[2]);
        File scoringMatrixFile = new File(args[3]);

        /*File base = new File("/work/fishjord/other_projects/classifier_v5/woojun_walsh");
        String region = args[0];

        File seqFile = new File(base, region + "_derep.fasta");
        File outputMatrix = new File(base, region + "_matrix.bin");
        File idMappingFile = new File(base, region + "_ids.txt");
        File scoringMatrixFile = new File(base, "NUC.4.4");*/

        DistanceModel model = new IdentityDistanceModel(true);
        ScoringMatrix scoringMatrix = ScoringMatrix.getDefaultNuclMatrix();
        Map<String, Integer> reverseMapping = IdMapping.fromFile(idMappingFile).getReverseMapping();
        int minOverlap = 25;

        SequenceFile inputSeqs = readSeqs(seqFile, null);
        int seqCount = inputSeqs.seqs.size();

        System.out.println("Read in " + seqCount + " " + inputSeqs.type + " sequences from " + seqFile);

        if (seqCount > 4000) {
            System.err.println("I was too lazy to write the pairwise aligner tool to work with large numbers of sequeneces (so many it can't fit in memory) so I set this arbitrary limit of 10000 sequences...gomenne");
            return;
        }

        List<ThinEdge> thinEdges = new ArrayList();

        long startTime = System.currentTimeMillis();
        long time = System.currentTimeMillis();

        final int interval = 10000;

        for (int seq1Index = 0; seq1Index < inputSeqs.seqs.size(); seq1Index++) {
            Sequence seq1 = inputSeqs.seqs.get(seq1Index);
            String seq1Str = SeqUtils.getUnalignedSeqString(seq1.getSeqString());

            for (int seq2Index = seq1Index + 1; seq2Index < inputSeqs.seqs.size(); seq2Index++) {
                Sequence seq2 = inputSeqs.seqs.get(seq2Index);
                String seq2Str = SeqUtils.getUnalignedSeqString(seq2.getSeqString());

                PairwiseAlignment result = PairwiseAligner.align(seq1Str, seq2Str, scoringMatrix, AlignmentMode.global);
                double distance = model.getDistance(result.getAlignedSeqi().getBytes(), result.getAlignedSeqj().getBytes(), minOverlap);

                thinEdges.add(new ThinEdge(reverseMapping.get(seq1.getSeqName()), reverseMapping.get(seq2.getSeqName()), (int) (distance * DistanceCalculator.MULTIPLIER)));

                if (thinEdges.size() % interval == 0) {
                    System.err.println("Computed " + interval + " thin edges in " + (System.currentTimeMillis() - time) + " ms (" + thinEdges.size() + " edges total in " + (System.currentTimeMillis() - startTime + " ms)"));
                    time = System.currentTimeMillis();
                }
            }
        }

        System.err.println("Computed " + thinEdges.size() + " thin edges in " + (System.currentTimeMillis() - startTime) + " ms");

        //Write edges sorts the edges for us
        startTime = System.currentTimeMillis();
        DistanceCalculator.writeEdges(thinEdges, outputMatrix);
        System.err.println("Wrote " + thinEdges.size() + " edges to " + outputMatrix + " in " + (System.currentTimeMillis() - startTime) + " ms");
    }
}
