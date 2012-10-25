/*
 * Ribosomal Database Project II
 * Copyright 2009 Michigan State University Board of Trustees
 */
package edu.msu.cme.rdp.hadoop.utils;

import edu.msu.cme.pyro.cluster.dist.ThinEdge;
import edu.msu.cme.pyro.cluster.io.EdgeReader;
import edu.msu.cme.rdp.hadoop.distance.mapred.keys.Comparison;
import edu.msu.cme.rdp.hadoop.distance.mapred.keys.IntDistance;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

/**
 *
 * @author farrisry
 */
public class HDFSEdgeReader implements EdgeReader {

    private List<Path> files;
    private SequenceFile.Reader seqFile;
    private IntDistance dist = new IntDistance();
    private Comparison comp = new Comparison();
    private Configuration config;
    private int nextPath = 0;

    public HDFSEdgeReader(Configuration config, List<Path> files) throws IOException {
        this.files = files;
        this.config = config;

        if (files == null || files.isEmpty() || config == null) {
            throw new IllegalArgumentException("Don't pass nulls to me please");
        }
        loadNextFile();
    }

    private void loadNextFile() throws IOException {
        if (nextPath < files.size()) {
            this.seqFile = new SequenceFile.Reader(files.get(nextPath).getFileSystem(config), files.get(nextPath), config);
        }
        nextPath++;
    }

    public ThinEdge nextThinEdge() throws IOException {
        if (seqFile.next(dist, comp)) {
            return new ThinEdge(comp.getFirst(), comp.getSecond(), dist.get());
        } else {
            if (nextPath < files.size()) {
                loadNextFile();
                return nextThinEdge();
            } else {
                return null;
            }
        }
    }

    public void close() throws IOException {
        seqFile.close();
    }
}