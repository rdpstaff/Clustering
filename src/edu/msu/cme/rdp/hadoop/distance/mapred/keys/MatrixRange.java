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
import org.apache.hadoop.io.WritableComparable;

/**
 * Indicates a square subsection of a larger matrix.
 * 
 * @author farrisry
 */
public class MatrixRange implements WritableComparable {

    public long x, y, offset;

    public MatrixRange() {
    }

    public void set(long x, long y, long offset) {
        this.x = x;
        this.y = y;
        this.offset = offset;
    }

    public long getOffset() {
        return offset;
    }

    public long getX() {
        return x;
    }

    public long getY() {
        return y;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(x);
        out.writeLong(y);
        out.writeLong(offset);
    }

    public void readFields(DataInput in) throws IOException {
        x = in.readLong();
        y = in.readLong();
        offset = in.readLong();
    }

    public int compareTo(Object o) {
        MatrixRange mr = (MatrixRange)o;
        if (x < mr.x) {
            return 1;
        } else if (x > mr.x) {
            return -1;
        } else {
            if (y < mr.y) {
                return 1;
            } else if (y > mr.y) {
                return -1;
            } else {
                if (offset < mr.offset) {
                    return 1;
                } else if (offset > mr.offset) {
                    return -1;
                } else {
                    return 0;
                }
            }
        }
    }

}
