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

package edu.msu.cme.rdp.taxatree;

import edu.msu.cme.pyro.cluster.dist.DistanceCalculator;
import edu.msu.cme.pyro.derep.IdMapping;
import edu.msu.cme.rdp.taxatree.utils.NewickPrintVisitor;
import edu.msu.cme.rdp.taxatree.utils.NewickPrintVisitor.NewickDistanceFactory;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.Options;

/**
 *
 * @author fishjord
 */
public class TreeBuilder {

    public static void main(String[] args) throws IOException {
        if(args.length != 3) {
            System.err.println("USAGE: TreeBuilder <idmapping> <merges.bin> <newick_out>");
            return;
        }

        IdMapping<Integer> idMapping = IdMapping.fromFile(new File(args[0]));
        DataInputStream mergeStream = new DataInputStream(new BufferedInputStream(new FileInputStream(args[1])));
        TaxonHolder lastMerged = null;
        int taxid = 0;
        final Map<Integer, Double> distMap = new HashMap();
        Map<Integer, TaxonHolder> taxonMap = new HashMap();

        try {
            while(true) {
                if(mergeStream.readBoolean()) { // Singleton
                    int cid = mergeStream.readInt();
                    int intId = mergeStream.readInt();
                    TaxonHolder<Taxon> holder;

                    List<String> seqids = idMapping.getIds(intId);
                    if(seqids.size() == 1) {
                        holder = new TaxonHolder(new Taxon(taxid++, seqids.get(0), ""));
                    } else {
                        holder = new TaxonHolder(new Taxon(taxid++, "", ""));
                        for(String seqid : seqids) {
                            int id = taxid++;
                            distMap.put(id, 0.0);
                            TaxonHolder th = new TaxonHolder(new Taxon(id, seqid, ""));
                            th.setParent(holder);
                            holder.addChild(th);
                        }
                    }

                    lastMerged = holder;
                    taxonMap.put(cid, holder);
                } else {
                    int ci = mergeStream.readInt();
                    int cj = mergeStream.readInt();
                    int ck = mergeStream.readInt();
                    double dist = (double)mergeStream.readInt() / DistanceCalculator.MULTIPLIER;

                    TaxonHolder holder = new TaxonHolder(new Taxon(taxid++, "", ""));

                    taxonMap.put(ck, holder);
                    holder.addChild(taxonMap.get(ci));
                    taxonMap.get(ci).setParent(holder);
                    distMap.put(ci, dist);
                    holder.addChild(taxonMap.get(cj));
                    taxonMap.get(cj).setParent(holder);
                    distMap.put(cj, dist);

                    lastMerged = holder;
                }
            }
        } catch(EOFException e) {

        }

        if(lastMerged == null) {
            throw new IOException("No merges in file");
        }

        PrintStream newickTreeOut = new PrintStream(new File(args[2]));
        NewickPrintVisitor visitor = new NewickPrintVisitor(newickTreeOut, false, new NewickDistanceFactory() {

            public float getDistance(int i) {
                return distMap.get(i).floatValue();
            }

        });

        lastMerged.biDirectionDepthFirst(visitor);
        newickTreeOut.close();
    }
}
