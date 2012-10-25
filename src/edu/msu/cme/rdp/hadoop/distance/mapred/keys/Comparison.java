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

package edu.msu.cme.rdp.hadoop.distance.mapred.keys;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author farrisry
 */
public class Comparison implements Writable {

    int first, second;

    public Comparison() {
    }

    public void set(int f, int s) {
        first = f;
        second = s;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(first);
        out.writeInt(second);
    }

    public void readFields(DataInput in) throws IOException {
        first = in.readInt();
        second = in.readInt();
    }

    public int getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }

}
