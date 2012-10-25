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
package edu.msu.cme.pyro.derep;

import edu.msu.cme.rdp.readseq.readers.SequenceReader;
import edu.msu.cme.rdp.readseq.readers.Sequence;
import edu.msu.cme.rdp.readseq.writers.FastaWriter;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

/**
 *
 * @author fishjord
 */
public class ExplodeMappings {

    private static Options options = new Options();

    static {
        options.addOption("w", "warn-only", false, "Only warn on errors, don't fail");
        options.addOption("o", "out-dir", true, "Write output files to this directory (default = explode)");
    }

    public static void main(String[] args) throws Exception {
        File outDir = new File("explode").getAbsoluteFile();
        IdMapping<Integer> idMapping;
        SampleMapping<String> sampleMapping;
        Map<String, FastaWriter> sampleToFileMap = new HashMap();
        boolean warnOnly = false;
        Set<String> seen = new HashSet();

        try {
            CommandLine line = new PosixParser().parse(options, args);

            if (line.hasOption("out-dir")) {
                outDir = new File(line.getOptionValue("out-dir"));
            }

            if (line.hasOption("warn-only")) {
                warnOnly = true;
            }

            args = line.getArgs();

            if (args.length != 3) {
                throw new Exception("Id mapping, sample mapping and input sequence file must be supplied");
            }

            idMapping = IdMapping.fromFile(new File(args[0]));
            Map<String, Integer> reverseMapping = idMapping.getReverseMapping();
            sampleMapping = SampleMapping.fromFile(new File(args[1]));

            System.out.println("Exploding " + reverseMapping.size() + " total sequences (" + idMapping.size() + " unique) into " + sampleMapping.getSampleList().size() + " sample files");

            SequenceReader seqReader = new SequenceReader(new File(args[2]));
            Sequence seq;

            if (!outDir.exists()) {
                if (!outDir.mkdirs()) {
                    throw new IOException("Couldn't make output dir " + outDir.getAbsolutePath());
                }
            }

            Set<Sequence> echoSeqs = new HashSet();
            while ((seq = seqReader.readNextSequence()) != null) {
                if (seq.getSeqName().startsWith("#")) {
                    echoSeqs.add(seq);
                    continue;
                }

                List<String> replicateIds = null;
                Integer id;
                if (seq.getSeqName().matches("\\d+") && !reverseMapping.containsKey(seq.getSeqName())) {
                    id = Integer.valueOf(seq.getSeqName());
                    replicateIds = idMapping.getIds(id);
                } else {
                    id = reverseMapping.get(seq.getSeqName());
                    if (id == null) {
                        if (warnOnly) {
                            System.err.println("No id mapping found for sequence " + seq.getSeqName() + ", assuming unique");
                            replicateIds = Arrays.asList(seq.getSeqName());
                        } else {
                            throw new Exception("Failed to find id mapping for " + seq.getSeqName());
                        }
                    } else {
                        replicateIds = idMapping.getIds(id);
                    }
                }

                if (replicateIds.indexOf(seq.getSeqName()) != 0) {
                    throw new IllegalStateException(seq.getSeqName() + " is not the exemplar of id " + id + ", refresh mappings can only be used with dereplicated sequence files");
                }

                for (String seqid : replicateIds) {
                    if (seen.contains(seqid)) {
                        System.err.println("Weird, I've already seen " + seqid);
                        continue;
                    }

                    seen.add(seqid);
                    String sample;
                    if((sample = sampleMapping.getSampleById(seqid)) != null) {
                        sample = new File(sampleMapping.getSampleById(seqid)).getName();
                    } else {
                        sample = "no_sample";
                    }

                    if (!sampleToFileMap.containsKey(sample)) {
                        sampleToFileMap.put(sample, new FastaWriter(new File(outDir, sample + ".fasta")));
                    }

                    sampleToFileMap.get(sample).writeSeq(seqid, seq.getSeqString());
                }
            }

            for (Sequence s : echoSeqs) {
                for (String sample : sampleToFileMap.keySet()) {
                    sampleToFileMap.get(sample).writeSeq(s);
                }
            }

        } catch (Exception e) {
            new HelpFormatter().printHelp("Clustering [options] <idmapping> <sample_mapping> <seq_file>", options);
            System.out.println("Error: " + e.getMessage());
            e.printStackTrace();
            return;
        } finally {
            for (FastaWriter writer : sampleToFileMap.values()) {
                try {
                    writer.close();
                } catch (Exception ignore) {
                }
            }
        }
    }
}
