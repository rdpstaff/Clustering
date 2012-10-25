/*
 * Ribosomal Database Project II  http://rdp.cme.msu.edu
 * Copyright 2009, Michigan State University Board of Trustees
 *
 * This software is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * The Ribosomal Database Project II and Michigan State University
 * distributes this software in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this software.  If not, see <http://www.gnu.org/licenses/>.
 */

package edu.msu.cme.rdp.hadoop.utils;

import edu.msu.cme.rdp.readseq.utils.SeqUtils;

/**
 * A sequence that has an integer as an ID instead of a string.
 *
 * @author farrisry
 */
public class IntSeq {

    final int id;
    final byte[] bases;

    public IntSeq(int id, byte[] bases) {
        this.bases = bases;
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public static IntSeq createSeq(int id, String bases) {
        return new IntSeq(id, SeqUtils.toBytes(bases));
    }

    public byte[] getBytes() {
        return bases;
    }
}
