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

import edu.msu.cme.pyro.derep.Dereplicator;
import edu.msu.cme.pyro.cluster.dist.DistanceCalculator;
import edu.msu.cme.pyro.cluster.dist.ThinEdge;
import edu.msu.cme.pyro.cluster.io.ClusterToBiom;
import edu.msu.cme.pyro.cluster.utils.AlignSeqMatch;
import edu.msu.cme.pyro.cluster.io.LocalEdgeReader;
import edu.msu.cme.pyro.cluster.io.RDPClustParser;
import edu.msu.cme.pyro.cluster.io.RFormatter;
import edu.msu.cme.pyro.cluster.utils.RepresenativeSeqs;
import edu.msu.cme.pyro.derep.ExplodeMappings;
import edu.msu.cme.pyro.derep.IdMapping;
import edu.msu.cme.pyro.derep.RefreshMappings;
import edu.msu.cme.pyro.derep.SampleMapping;
import edu.msu.cme.rdp.hadoop.HadoopMain;
import edu.msu.cme.rdp.readseq.MaskSequenceNotFoundException;
import edu.msu.cme.rdp.readseq.readers.IndexedSeqReader;
import edu.msu.cme.rdp.readseq.readers.SequenceReader;
import edu.msu.cme.rdp.readseq.readers.SeqReader;
import edu.msu.cme.rdp.readseq.readers.Sequence;
import edu.msu.cme.rdp.readseq.utils.SeqUtils;
import edu.msu.cme.rdp.readseq.writers.FastaWriter;
import edu.msu.cme.rdp.taxatree.TreeBuilder;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author fishjord
 */
public class ClusterMain {

    private static final boolean hadoop;

    static {
        boolean t = false;
        try {
            Class.forName("edu.msu.cme.rdp.hadoop.HadoopMain");
            t = true;
        } catch (ClassNotFoundException e) {
        }

        hadoop = t;
    }

    private static void demultiplex(String[] args) throws IOException {
        if (args.length != 4 && args.length != 5) {
            System.out.println("USAGE: demultiplex [id-mapping] <sample-mapping> <result_file> <tab_index> <output_directory>");
            System.exit(1);
        }

        int argIndex = 0;
        IdMapping<Integer> idMapping = null;
        Map<String, Integer> reverseMapping = null;
        if (args.length == 5) {
            idMapping = IdMapping.fromFile(new File(args[argIndex++]));
            reverseMapping = idMapping.getReverseMapping();
        }

        SampleMapping<String> sampleMapping = SampleMapping.fromFile(new File(args[argIndex++]));
        BufferedReader reader = new BufferedReader(new FileReader(args[argIndex++]));
        int tabIndex = Integer.valueOf(args[argIndex++]);
        File outDir = new File(args[argIndex++]);

        String line;

        Map<String, PrintStream> outFiles = new HashMap();
        for (String sample : sampleMapping.getSampleList()) {
            outFiles.put(sample, new PrintStream(new File(outDir, sample + ".txt")));
        }

        while ((line = reader.readLine()) != null) {
            String[] lexemes = line.split("\t");
            if (lexemes.length <= tabIndex) {
                continue;
            }

            String seqid = lexemes[tabIndex];
            String sample = sampleMapping.getSampleById(seqid);
            if (sample == null) {
                System.out.println("No sample for id " + seqid);
            } else {
                List<String> seqids;
                if (idMapping != null) {
                    Integer exid = reverseMapping.get(seqid);
                    if (exid == null) {
                        System.out.println("No id mapping found for " + seqid);
                        continue;
                    }
                    seqids = idMapping.getIds(exid);
                } else {
                    seqids = Arrays.asList(seqid);
                }

                for (String sid : seqids) {
                    StringBuilder printLine = new StringBuilder();
                    for (int index = 0; index < lexemes.length; index++) {
                        if (index == tabIndex) {
                            printLine.append(sid);
                        } else {
                            printLine.append(lexemes[index]);
                        }
                    }

                    outFiles.get(sample).println(line);
                }
            }
        }

        for (PrintStream out : outFiles.values()) {
            out.close();
        }
    }

    private static void toSquareMatrix(String[] newArgs) throws IOException {

        if (newArgs.length != 3) {
            System.err.println("USAGE: <column matrix> <idmapping> <output matrix>");
            return;
        }

        LocalEdgeReader is = new LocalEdgeReader(new File(newArgs[0]));
        IdMapping<Integer> idMapping = IdMapping.fromFile(new File(newArgs[1]));
        if (idMapping.size() > 5000) {
            System.err.println("I refuse to convert a file with more than 5000 sequences");
            return;
        }

        double[][] matrix = new double[idMapping.size()][idMapping.size()];

        for (int row = 0; row < matrix.length; row++) {
            matrix[row][row] = 0;
            for (int col = row + 1; col < matrix.length; col++) {
                matrix[row][col] = matrix[col][row] = -1;
            }
        }

        ThinEdge edge;
        while ((edge = is.nextThinEdge()) != null) {
            matrix[edge.getSeqj()][edge.getSeqi()] = matrix[edge.getSeqi()][edge.getSeqj()] = edge.getDist() / (double) DistanceCalculator.MULTIPLIER;
        }
        is.close();

        PrintStream out = new PrintStream(new File(newArgs[2]));
        out.println(matrix.length);
        for (int row = 0; row < matrix.length; row++) {
            for (String seqid : idMapping.getIds(row)) {
                out.print(seqid + "  ");
                for (int col = 0; col < matrix.length; col++) {
                    for (String colid : idMapping.getIds(col)) {
                        out.print(matrix[row][col] + " ");
                    }
                }
                out.println();
            }
        }
        out.close();
    }

    private static void removeSeqs(String[] args) throws IOException {
        if (args.length != 2 && args.length != 3) {
            System.err.println("USAGE: <ids_file> <seq_file> [remove]");
            return;
        }

        boolean remove = true;
        File idFile = new File(args[0]);
        File seqFile = new File(args[1]);

        if (args.length == 3) {
            remove = Boolean.parseBoolean(args[2]);
        }

        Set<String> ids = new HashSet();

        BufferedReader reader = new BufferedReader(new FileReader(idFile));
        String line;
        while ((line = reader.readLine()) != null) {
            ids.add(line.trim());
        }
        reader.close();

        SeqReader seqReader = new SequenceReader(seqFile);
        Sequence seq;

        FastaWriter out = new FastaWriter(System.out);
        while ((seq = seqReader.readNextSequence()) != null) {
            boolean idMatch = ids.contains(seq.getSeqName());

            if (idMatch && !remove) {
                out.writeSeq(seq);
            } else if (!idMatch && remove) {
                out.writeSeq(seq);
            }
        }
        out.close();
    }

    private static void toFasta(String[] args) throws IOException, MaskSequenceNotFoundException {
        if (args.length != 1 && args.length != 2) {
            System.err.println("USAGE: to-fasta <input-file> [mask-seqid]");
            return;
        }

        SeqReader reader = null;
        int totalSeqs = 0;

        if (args[0].equals("-")) {
            reader = new SequenceReader(System.in);
        } else {
            File seqFile = new File(args[0]);

            if (args.length == 1) {
                reader = new SequenceReader(seqFile);
            } else {
                reader = new IndexedSeqReader(seqFile, args[1]);
            }
        }

        FastaWriter out = new FastaWriter(System.out);
        Sequence seq;

        long startTime = System.currentTimeMillis();
        while ((seq = reader.readNextSequence()) != null) {
            out.writeSeq(seq.getSeqName().replace(" ", "_"), seq.getDesc(), seq.getSeqString());
            totalSeqs++;
        }

        System.err.println("Converted " + totalSeqs + " sequences from " + args[0] + " (" + reader.getFormat() + ") to fasta in " + (System.currentTimeMillis() - startTime) / 1000 + " s");
        out.close();
    }

    private static void toUnalignedFasta(String[] args) throws IOException, MaskSequenceNotFoundException {
        if (args.length == 0) {
            System.err.println("USAGE: to-unaligned-fasta <input-file>...");
            return;
        }

        SeqReader reader = null;
        int totalSeqs = 0;

        FastaWriter out = new FastaWriter(System.out);
        Sequence seq;
        long startTime = System.currentTimeMillis();

        for (int index = 0; index < args.length; index++) {
            if (args[index].equals("-")) {
                reader = new SequenceReader(System.in);
            } else {
                File seqFile = new File(args[index]);
                reader = new SequenceReader(seqFile);
            }

            while ((seq = reader.readNextSequence()) != null) {
                out.writeSeq(seq.getSeqName().replace(" ", "_"), seq.getDesc(), SeqUtils.getUnalignedSeqString(seq.getSeqString()));
                totalSeqs++;
            }

        }

        System.err.println("Converted " + totalSeqs + " sequences from " + Arrays.asList(args) + " (" + reader.getFormat() + ") to fasta in " + (System.currentTimeMillis() - startTime) / 1000 + " s");
        out.close();
    }

    private static void convertClusterBiom(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("USAGE: <cluster_file> <cutoff>");
            return;
        }

        File clusterFile = new File(args[0]);
        String cutoff = args[1];

        RDPClustParser parser = new RDPClustParser(clusterFile);
        ClusterToBiom.writeCutoff(parser.getCutoff(cutoff), System.out);
        parser.close();
    }
    
    private static void convertClusterRformat(String[] args) throws IOException {
        if (args.length != 4 && args.length != 5) {
            throw new IllegalArgumentException("Usage: clusterFile outdir startDist endDist [idcountmap]" 
            + "\n idcountmap file contains the seqID and count separated by space or tab. Decimals allowed."
             + "\n This can be used to adjust the sequence abundance by coverage (Xander assembly)");
        }

        File clusterFile = new File(args[0]);
        File userTempDir = new File(args[1]);
        Double startDist = Double.parseDouble(args[2]);
        Double endDist = Double.parseDouble(args[3]);
        File idcountmapFile = null;
        if ( args.length == 5){
            idcountmapFile = new File(args[4]);
        }

        RFormatter.createTabulatedFormatForRange(clusterFile, startDist, endDist, userTempDir, idcountmapFile);
        
    }

    public static void printUsage() {
        System.err.println("USAGE: Main <command name> command args...");
        System.err.println("\tCommands: derep, dmatrix, cluster, dump-edges, convert-column-matrix" + (hadoop ? " hadoop-distance" : ""));
        System.err.println("\tderep            - Dereplicate sequence file");
        System.err.println("\tdmatrix          - Compute distance matrix from aligned sequence file");
        System.err.println("\tpairwise         - Compute distance matrix from unaligned sequence file (very slow, limited to 4k sequences)");
        System.err.println("\tcluster          - Cluster a distance file");
        System.err.println("\talign-seq-match  - Aligned sequence match");
        System.err.println("\trep-seqs         - Get represenative sequences from a cluster file");
        System.err.println("\texplode-mappings - Explode a dereplicated sequence file back to sample replicated files");
        System.err.println("\tdemultiplex      - Demultiplex a tab-delimited result file using an id and sample mapping");
        System.err.println("\trefresh-mappings - Remove mapping entries for sequences externally filtered");
        System.err.println("\tdump-edges       - Dumps a binary distance file to flat text (stdout)");
        System.err.println("\tsquare-matrix    - Dumps a binary distance file to a square matrix");
        System.err.println("\ttree             - Converts a merges file to a newick tree");
        System.err.println("\treplay-cluster   - Replays a merge file to create a cluster file");
        System.err.println("\tto-fasta         - Convert a sequence file to fasta format");
        System.err.println("\tto-unaligned-fasta         - Convert a sequence file to fasta format");
        System.err.println("\tfilter-seqs      - Remove sequences from a file");
        System.err.println("\tcluster-to-biom  - Convert a cluster file to a biom otu table");
        System.err.println("\tcluster_to_Rformat  - Convert a cluster file to a R compatible community data matrix file");
        if (hadoop) {
            System.err.println("\thadoop - Calculate distances using hadoop distance calculator");
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printUsage();
            return;
        }

        String commandName = args[0];
        String[] newArgs = Arrays.copyOfRange(args, 1, args.length);

        if (commandName.equals("derep")) {
            Dereplicator.main(newArgs);
        } else if (commandName.equals("dmatrix")) {
            DistanceCalculator.main(newArgs);
        } else if (commandName.equals("cluster")) {
            Clustering.main(newArgs);
        } else if (commandName.equals("rep-seqs")) {
            RepresenativeSeqs.main(newArgs);
        } else if (commandName.equals("explode-mappings")) {
            ExplodeMappings.main(newArgs);
        } else if (commandName.equals("refresh-mappings")) {
            RefreshMappings.main(newArgs);
        } else if (commandName.equals("dump-edges")) {
            LocalEdgeReader is = new LocalEdgeReader(new File(newArgs[0]));
            ThinEdge edge;
            while ((edge = is.nextThinEdge()) != null) {
                System.out.println(edge.getSeqi() + "\t" + edge.getSeqj() + "\t" + ((float) edge.getDist() / DistanceCalculator.MULTIPLIER));
            }
            is.close();
        } else if (commandName.equals("square-matrix")) {
            toSquareMatrix(newArgs);
        } else if (hadoop && commandName.equals("hadoop")) {
            HadoopMain.main(newArgs);
        } else if (commandName.equals("tree")) {
            TreeBuilder.main(newArgs);
        } else if (commandName.equals("replay-cluster")) {
            ClusterReplay.main(newArgs);
        } else if (commandName.equals("demultiplex")) {
            demultiplex(newArgs);
        } else if (commandName.equals("align-seq-match")) {
            AlignSeqMatch.main(newArgs);
        } else if (commandName.equals("to-fasta")) {
            toFasta(newArgs);
        } else if (commandName.equals("to-unaligned-fasta")) {
            toUnalignedFasta(newArgs);
        } else if (commandName.equals("filter-seqs")) {
            removeSeqs(newArgs);
        } else if (commandName.equals("cluster-to-biom")) {
            convertClusterBiom(newArgs);
        } else if (commandName.equals("cluster_to_Rformat")) {
            convertClusterRformat(newArgs);
        }else {
            printUsage();
        }
    }
}
