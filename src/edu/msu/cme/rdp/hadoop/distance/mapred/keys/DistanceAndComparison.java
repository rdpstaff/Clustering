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
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * @author farrisry
 */
public class DistanceAndComparison implements WritableComparable<DistanceAndComparison> {

    public int distance; // dist * 10,000
    public int first;
    public int second;

    public DistanceAndComparison() {
    }

    public void setDistance(int distance) {
        this.distance = distance;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(distance);
        out.writeInt(first);
        out.writeInt(second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        distance = in.readInt();
        first = in.readInt();
        second = in.readInt();
    }

    @Override
    public int compareTo(DistanceAndComparison o) {
        if (distance < o.distance) {
            return -1;
        } else if (distance > o.distance) {
            return 1;
        } else {
            if (first < o.first) {
                return -1;
            } else if (first > o.first) {
                return 1;
            } else {
                if (second < o.second) {
                    return -1;
                } else if (second > o.second) {
                    return 1;
                } else {
                    return 0;
                }
            }
        }
    }

    public static class GroupingComparator implements RawComparator<DistanceAndComparison> {

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            int dist1 = WritableComparator.readInt(b1, s1);
            int dist2 = WritableComparator.readInt(b2, s2);
            if (dist1 == dist2) {
                return 0;
            } else if (dist1 < dist2) {
                return -1;
            } else {
                return 1;
            }
        }

        @Override
        public int compare(DistanceAndComparison o1, DistanceAndComparison o2) {
            return o1.compareTo(o2);
        }
    }

}
