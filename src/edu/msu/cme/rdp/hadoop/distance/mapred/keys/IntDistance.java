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

import org.apache.hadoop.io.IntWritable;

/**
 * Stores distances as an int, and provides utility methods for converting
 * to/from floating point.
 * @author farrisry
 */
public class IntDistance extends IntWritable {

    public static final int SCALE_FACTOR = 10000;

    public static int scale(double distance) {
        return Math.round(SCALE_FACTOR * new Double(distance).floatValue());
    }

    public static double unscale(int distance) {
        return distance / (double)SCALE_FACTOR;
    }

    public static double unscaleToFloat(int distance) {
        return distance / (float)SCALE_FACTOR;
    }

    public IntDistance() {
        super();
    }
}
