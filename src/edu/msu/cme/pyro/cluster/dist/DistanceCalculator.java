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

import edu.msu.cme.pyro.cluster.utils.ClusterUtils;
import edu.msu.cme.pyro.cluster.io.EdgeWriter;
import edu.msu.cme.pyro.derep.IdMapping;
import edu.msu.cme.rdp.alignment.pairwise.rna.DistanceModel;
import edu.msu.cme.rdp.alignment.pairwise.rna.IdentityDistanceModel;
import edu.msu.cme.rdp.alignment.pairwise.rna.OverlapCheckFailedException;
import edu.msu.cme.rdp.alignment.pairwise.rna.UncorrectedDistanceModel;
import edu.msu.cme.rdp.readseq.MaskSequenceNotFoundException;
import edu.msu.cme.rdp.readseq.SequenceType;
import edu.msu.cme.rdp.readseq.readers.Sequence;
import edu.msu.cme.rdp.readseq.readers.IndexedSeqReader;
import edu.msu.cme.rdp.readseq.utils.SeqUtils;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

/**
 *
 * @author fishjord
 */
public class DistanceCalculator {

    public static final int MULTIPLIER = 10000;

    private static class PartialMatrixResult {

        List<File> splitFiles = new ArrayList();
        double psi = Double.MIN_VALUE;
    }

    private static class SequenceFile {

        public Map<String, byte[]> sequenceMap = new HashMap();

        ;
        SequenceType type = SequenceType.Unknown;
    }
    private static final Options options = new Options();

    static {
        options.addOption(new Option("m", "mask", true, "Mask sequence"));
        options.addOption(new Option("c", "dist-cutoff", true, "Only save distances below the cutoff"));
        options.addOption(new Option("I", "in", true, "Input fasta file"));
        options.addOption(new Option("l", "min-overlap", true, "Minium number of comparable positions (default = 100)"));
        options.addOption(new Option("o", "outfile", true, "File to write sorted column matrix to"));
        options.addOption(new Option("i", "id-mapping", true, "Id mapping file"));
        options.addOption(new Option("w", "workdir", true, "Working directory where temp files are stored"));
    }

    private static SequenceFile readSeqs(File fastaFile, String maskSeq) throws IOException, MaskSequenceNotFoundException {
        IndexedSeqReader reader = null;
        if (maskSeq == null) {
            reader = new IndexedSeqReader(fastaFile);
        } else {
            reader = new IndexedSeqReader(fastaFile, maskSeq);
        }

        SequenceFile ret = new SequenceFile();

        List<Sequence> seqs = new ArrayList();
        List<String> seqids = reader.getSeqIds();

        for (String seqid : seqids) {
            Sequence seq = reader.readSeq(seqid);

            if (seqs.isEmpty()) {
                ret.type = SeqUtils.guessSequenceType(seq);
            }

            seqs.add(seq);
        }
        reader.close();

        if (ret.type == SequenceType.Nucleotide) {
            for (Sequence seq : seqs) {
                ret.sequenceMap.put(seq.getSeqName(), SeqUtils.toBytes(seq.getSeqString()));
            }
        } else {
            for (Sequence seq : seqs) {
                ret.sequenceMap.put(seq.getSeqName(), seq.getSeqString().getBytes());
            }
        }

        return ret;
    }

    /**
     * Writes edges out to the specified file, sorts the list of edges inplace (aka will reorder the supplied list)
     * @param edges
     * @param outFile
     * @throws IOException
     */
    public static void writeEdges(List<ThinEdge> edges, File outFile) throws IOException {
        Collections.sort(edges);

        EdgeWriter out = new EdgeWriter(outFile);
        for (ThinEdge edge : edges) {
            out.writeEdge(edge);
        }
        out.close();
    }

    private static PartialMatrixResult computePartialMatrices(Map<String, byte[]> seqs, Map<String, Integer> reverseMapping, File nonoverlapFile, File workingDir, DistanceModel model, int overlapLimit, double cutoff) throws IOException {
        Set<String> notFoundIds = new LinkedHashSet();
        List<String> seqids = new ArrayList(seqs.keySet());

        for (String seqid : seqids) {
            if (!reverseMapping.containsKey(seqid)) {
                notFoundIds.add(seqid);
            }
        }

        if (!notFoundIds.isEmpty()) {
            throw new IOException("Failed to find id mapping for sequence(s): " + notFoundIds);
        }

        PartialMatrixResult result = new PartialMatrixResult();

        List<ThinEdge> edges = new ArrayList();

        int edgeFileCount = 0;
        String seq1Name = null;
        String seq2Name = null;
        double dist;
        Integer seq1Id, seq2Id;
        File tmp;
        System.gc();
        long edgeCount = 0, edgesWritten = 0, nonOverlapCount = 0;
        DataOutputStream nonOverlapOut = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(nonoverlapFile)));

        for (int seq1Index = 0; seq1Index < seqids.size(); seq1Index++) {
            seq1Name = seqids.get(seq1Index);
            seq1Id = reverseMapping.get(seq1Name);

            for (int seq2Index = seq1Index + 1; seq2Index < seqids.size(); seq2Index++) {
                seq2Name = seqids.get(seq2Index);
                seq2Id = reverseMapping.get(seq2Name);

                edgeCount++;
                try {
                    dist = model.getDistance(seqs.get(seq1Name), seqs.get(seq2Name), overlapLimit);
                } catch (OverlapCheckFailedException e) {
                    nonOverlapCount++;
                    nonOverlapOut.writeInt(seq1Id);
                    nonOverlapOut.writeInt(seq2Id);
                    continue;
                }
                if (dist > result.psi) {
                    result.psi = dist;
                }

                if (dist < cutoff) {
                    edges.add(new ThinEdge(seq1Id, seq2Id, (int) ((dist * MULTIPLIER) + .5)));
                    edgesWritten++;
                }

                if (ClusterUtils.getMemRatio() > .5f) {
                    System.gc();
                    tmp = new File(workingDir, "partial_matrix" + (edgeFileCount++));
                    System.err.println("Dumping " + edges.size() + " edges to " + tmp.getName() + " (memory ratio=" + ClusterUtils.getMemRatio() + ")");
                    System.err.println("Edges computed=" + edgeCount + " edges written=" + edgesWritten + " nonoverlapping edges=" + nonOverlapCount);
                    result.splitFiles.add(tmp.getAbsoluteFile());
                    writeEdges(edges, tmp);
                    edges.clear();
                    System.gc();
                }
            }
        }

        nonOverlapOut.close();

        if (edges.size() > 0) {
            tmp = new File(workingDir, "partial_matrix" + (edgeFileCount++));
            System.err.println("Dumping " + edges.size() + " edges to " + tmp.getName() + " FINAL EDGES (memory ratio=" + ClusterUtils.getMemRatio() + ")");
            result.splitFiles.add(tmp.getAbsoluteFile());
            writeEdges(edges, tmp);
        }

        return result;
    }

    public static void main(String[] args) throws Exception {

        String mask = null;
        File workingDir = new File(".");
        File outFile;
        File seqFile;
        File idMappingFile;
        int overlapLimit = 100;
        double cutoff = 1.0f;
        DistanceModel model;

        try {
            CommandLine line = new PosixParser().parse(options, args);

            if (line.hasOption("mask")) {
                mask = line.getOptionValue("mask");
            }

            if (line.hasOption("outfile")) {
                outFile = new File(line.getOptionValue("outfile"));
            } else {
                throw new Exception("Output file must be specified");
            }

            if (line.hasOption("min-overlap")) {
                overlapLimit = new Integer(line.getOptionValue("min-overlap"));
                if (overlapLimit <= 0) {
                    System.err.println("Min-overlap must be > 0");
                }
            }

            if (line.hasOption("dist-cutoff")) {
                cutoff = new Double(line.getOptionValue("dist-cutoff"));
                if (cutoff < .05 || cutoff > 1.0) {
                    System.err.println("Distance cutoff must be between .05 and 1.0");
                }
            }

            if (line.hasOption("id-mapping")) {
                idMappingFile = new File(line.getOptionValue("id-mapping"));
            } else {
                throw new Exception("Id Mapping must be specified");
            }

            if (line.hasOption("workdir")) {
                workingDir = new File(line.getOptionValue("workdir"));
                if (!workingDir.isDirectory()) {
                    throw new Exception("workdir must be a directory");
                }
            }

            if (line.hasOption("in")) {
                seqFile = new File(line.getOptionValue("in"));
                if (!seqFile.exists()) {
                    throw new Exception(seqFile.getAbsolutePath() + " doesn't exist");
                }
            } else {
                throw new Exception("Input file must be specified");
            }

        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
            new HelpFormatter().printHelp("DistanceCalculator", options, true);
            return;
        }

        System.err.println("Reading sequences(memratio=" + ClusterUtils.getMemRatio() + ")...");
        SequenceFile seqs = readSeqs(seqFile, mask);
        int numSeqs = seqs.sequenceMap.size();

        if(seqs.type == SequenceType.Nucleotide) {
            model = new UncorrectedDistanceModel();
        } else {
            model = new IdentityDistanceModel();
        }

        System.err.println("Using distance model " + model.getClass().getCanonicalName());

        System.err.println("Read " + numSeqs + " " + seqs.type + " sequences (memratio=" + ClusterUtils.getMemRatio() + ")");
        System.err.println("Reading ID Mapping from file " + idMappingFile.getAbsolutePath());
        IdMapping<Integer> idMapping = IdMapping.fromFile(idMappingFile);

        if (idMapping.size() != numSeqs) {
            System.err.println("Something is wrong, read in ID mapping with " + idMapping.size() + " entries but I read in " + numSeqs + " sequences");
            System.exit(1);
        }

        Map<String, Integer> reverseMapping = idMapping.getReverseMapping();
        System.err.println("Read mapping for " + reverseMapping.size() + " sequences (memratio=" + ClusterUtils.getMemRatio() + ")");
        System.err.println("Starting distance computations, predicted max edges=" + ((long) numSeqs * numSeqs) + ", at=" + new Date());

        long startTime = System.currentTimeMillis();
        PartialMatrixResult result = computePartialMatrices(seqs.sequenceMap, reverseMapping, new File(workingDir, "nonoverlapping.bin"), workingDir, model, overlapLimit, cutoff);

        System.out.println("Matrix edges computed: " + (System.currentTimeMillis() - startTime));
        System.out.println("Maximum distance: " + result.psi);
        System.out.println("Splits: " + result.splitFiles.size());

        startTime = System.currentTimeMillis();
        new MergeDistsJob(result.splitFiles, outFile).run();
        System.out.println("Partition files merged: " + (System.currentTimeMillis() - startTime));
    }
}
