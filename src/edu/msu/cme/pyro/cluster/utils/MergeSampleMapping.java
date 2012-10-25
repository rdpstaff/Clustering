/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.msu.cme.pyro.cluster.utils;

import edu.msu.cme.pyro.derep.SampleMapping;
import java.io.File;

/**
 *
 * @author Jordan Fish <fishjord at msu.edu>
 */
public class MergeSampleMapping {

    public static void main(String[] args) throws Exception {
        if(args.length == 0) {
            System.err.println("USAGE: MergeSampleMapping <sample file>...");
            System.exit(1);
        }

        SampleMapping sampleMapping = null;

        for(String s : args) {
            System.err.println("Reading sample mapping from " + s);
            if(sampleMapping == null) {
                sampleMapping = SampleMapping.fromFile(new File(s));
            } else {
                SampleMapping.fromFile(new File(s), sampleMapping);
            }
        }

        sampleMapping.toStream(System.out);
    }

}
