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

package edu.msu.cme.pyro.cluster.utils;

/**
 *
 * @author fishjord
 */
public class ClusterUtils {
    public static final byte MISSING_DATA = Byte.MIN_VALUE;
    public static final int BYTE_OFFSET = 125; // offset to convert float distance to a byte (ceil(float * 100) - offset)
    public static final int STEP_SIZE = 5; // 5 steps per percent
    public static final int MAX_DIST = 250; // maximum cluster distance == 50%

    public static class ClusterParams {
        private byte maxDist;
        private byte step;

        public ClusterParams(byte maxDist, byte step) {
            this.maxDist = maxDist;
            this.step = step;
        }

        public byte getMaxDist() {
            return maxDist;
        }

        public byte getStep() {
            return step;
        }

    }

    public static ClusterParams convertClusterParams(float maxDist, float step) {
        int maxClusterDist = (int)(maxDist * ClusterUtils.STEP_SIZE) - ClusterUtils.BYTE_OFFSET;
        if (maxClusterDist >= ClusterUtils.MAX_DIST ) {
            throw new IllegalArgumentException("Maximum cluster distance <=" + ClusterUtils.MAX_DIST / ClusterUtils.STEP_SIZE);
        }
        byte s = (byte)(step * ClusterUtils.STEP_SIZE);
        if (s <= 0) {
            throw new IllegalArgumentException("Minimum cluster step =" + 1.0f / ClusterUtils.STEP_SIZE);
        }

        return new ClusterParams((byte)maxClusterDist, s);
    }

    public static double getMemRatio() {
        return (double)(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / Runtime.getRuntime().maxMemory();
    }
}
