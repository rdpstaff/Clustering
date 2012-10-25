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
import java.io.File;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;

/**
 *
 * @author fishjord
 */
public class RefreshMappings {

    public static void main(String [] args) throws Exception {
        if(args.length != 5) {
            System.err.println("USAGE: <altered_seq_file> <original_id_mapping> <original_sample_mapping> <out_id_mapping> <out_sample_mapping>");
            return;
        }

        SequenceReader reader = new SequenceReader(new File(args[0]));

        IdMapping<Integer> originalIds = IdMapping.fromFile(new File(args[1]));
        Map<String, Integer> reverseIdMapping = originalIds.getReverseMapping();
        SampleMapping originalSamples = SampleMapping.fromFile(new File(args[2]));

        IdMapping<Integer> newIds = new IdMapping();
        SampleMapping newSamples = new SampleMapping();

        Sequence seq;

        System.out.println("Original id mapping contains " + originalIds.size() + " entries");
        System.out.println("Original sample mapping contains " + originalSamples.getSampleList().size() + " samples and " + originalSamples.getIdToSampleMap().size() + " entries");

        while((seq = reader.readNextSequence()) != null) {
            if(seq.getSeqName().startsWith("#")) {
                continue;
            }
            
            if(!reverseIdMapping.containsKey(seq.getSeqName())) {
                throw new RuntimeException(seq.getSeqName() + " not in the original id mapping");
            }
            Integer id = reverseIdMapping.get(seq.getSeqName());
            List<String> replicateIds = originalIds.getIds(id);

            if(replicateIds.indexOf(seq.getSeqName()) != 0) {
                throw new IllegalStateException(seq.getSeqName() + " is not the exemplar of id " + id + ", refresh mappings can only be used with dereplicated sequence files");
            }

            for(String seqid : replicateIds) {
                newIds.addId(id, seqid);
                newSamples.addSeq(originalSamples.getSampleById(seqid), seqid);
            }
        }

        PrintStream out = new PrintStream(args[3]);
        newIds.toStream(out);
        out.close();

        out = new PrintStream(args[4]);
        newSamples.toStream(out);
        out.close();
    }
}
