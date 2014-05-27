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
package edu.msu.cme.rdp.hadoop;

import edu.msu.cme.pyro.derep.IdMapping;
import edu.msu.cme.rdp.hadoop.distance.DistancesMain;
import edu.msu.cme.rdp.hadoop.distance.sampler.SamplerMain;
import edu.msu.cme.rdp.hadoop.utils.AlignedIntSeqStore;
import edu.msu.cme.rdp.hadoop.utils.HadoopClustering;
import edu.msu.cme.rdp.hadoop.utils.IntSeq;
import edu.msu.cme.rdp.readseq.readers.IndexedSeqReader;
import edu.msu.cme.rdp.readseq.readers.SequenceReader;
import edu.msu.cme.rdp.readseq.readers.SeqReader;
import edu.msu.cme.rdp.readseq.readers.Sequence;
import java.io.File;
import java.util.Arrays;
import java.util.Map;

/**
 *
 * @author fishjord
 */
public class HadoopMain {

    public static void main(String... args) throws Exception {

        if (args.length == 0) {
            System.err.println("USAGE: HadoopMain <cmd> <cmd_args>");
            System.err.println("cmds:");
            System.err.println("\tbin-seqs - Creates binary sequence file (used in distance calculator)");
            System.err.println("\tsample - Sample sequence file for determining cutoffs (must be run before dmatrix) \n\t\t(must be run with hadoop runner, input files must be in hdfs, output files are placed on hdfs)");
            System.err.println("\tdmatrix - Creates a distance matrix from a binary sequence file \n\t\t(must be run with hadoop runner, input files must be in hdfs, output files are placed on hdfs)");
            System.err.println("\tcluster - Run complete linkage clustering (must be run with hadoop runner, input and output files on hdfs)");
            System.err.println("\tdump-edges - Dump edges to standard out (must be run with hadoop runner, input file matrix on hdfs, mappings and output on local fs)");
            System.err.println("\tdump-bin - Dump edges to a binary file compatible with the non-hadoop run clustering program (must be run with hadoop runner, input file matrix on hdfs, mappings and output on local fs)");
            System.exit(1);
        }

        String cmd = args[0];
        args = Arrays.copyOfRange(args, 1, args.length);

        if (cmd.equals("bin-seqs")) {
            if (args.length != 3 && args.length != 4) {
                System.err.println("USAGE: bin-seqs <seq_file> <id-mapping> <out_file> [mask-seq-id]");
                System.exit(1);
            }

            SeqReader reader = null;
            if (args.length == 4) {
                reader = new IndexedSeqReader(new File(args[0]), args[3]);
            } else {
                reader = new SequenceReader(new File(args[0]));
            }

            Map<String, Integer> mapping = IdMapping.fromFile(new File(args[1])).getReverseMapping();
            AlignedIntSeqStore store = new AlignedIntSeqStore(new File(args[2]));
            try {
                Sequence seq;

		while((seq = reader.readNextSequence()) != null) {
                    try {
                        store.addSeq(IntSeq.createSeq(mapping.get(seq.getSeqName()), seq.getSeqString()));
                    } catch (Exception e) {
                        System.out.println("\"" + seq.getSeqString() + "\"");
                        throw e;
                    }
                }
            } finally {
                store.close();
            }
        } else if (cmd.equals("sample")) {
            SamplerMain.main(args);
        } else if (cmd.equals("dmatrix")) {
            DistancesMain.main(args);
        } else if (cmd.equals("cluster")) {
            HadoopClustering.main(args);
        } else if (cmd.equals("dump-edges")) {
            HadoopClustering.dumpDists(args);
        } else if (cmd.equals("dump-bin")) {
            HadoopClustering.dumpBinDists(args);
        }
    }
}
