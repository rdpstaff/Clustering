/*
 * Ribosomal Database Project II
 * Copyright 2009 Michigan State University Board of Trustees
 */
package edu.msu.cme.pyro.derep;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

/**
 *
 * @author farrisry
 */
public class SampleMapping<E> {

    private Map<String, Set<E>> sampleToIds = new LinkedHashMap<String, Set<E>>();
    private Map<E, String> idToSample = null;

    public void addSeq(String sample, E id) {
        Set<E> sampleIds = sampleToIds.get(sample);
        if (sampleIds == null) {
            sampleIds = new HashSet<E>();
            sampleToIds.put(sample, sampleIds);
        }
        sampleIds.add(id);
    }

    public List<String> getSampleList() {
        return new ArrayList<String>(sampleToIds.keySet());
    }
    Map<String, Set<E>> sampleToIdSet = new HashMap<String, Set<E>>();

    public Set<E> getIdsBySample(String sample) {
        if (sampleToIdSet.get(sample) == null) {
            sampleToIdSet.put(sample, new HashSet<E>(sampleToIds.get(sample)));
        }
        return sampleToIdSet.get(sample);
    }

    public String getSampleById(E id) {
        if (idToSample == null) {
            idToSample = getIdToSampleMap();
        }
        return idToSample.get(id);
    }

    public Map<E, String> getIdToSampleMap() {
        Map<E, String> idToSample = new HashMap<E, String>();
        for (String sample : sampleToIds.keySet()) {
            for (E id : sampleToIds.get(sample)) {
                idToSample.put(id, sample);
            }
        }
        return idToSample;
    }

    public void toStream(PrintStream out) {
        for (String sample : sampleToIds.keySet()) {
            for (E id : sampleToIds.get(sample)) {
                out.println(id + "\t" + sample);
            }
        }
        out.flush();
        out.close();
    }

    public static SampleMapping<String> fromFile(File file) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line;
        SampleMapping<String> map = new SampleMapping<String>();
        while ((line = reader.readLine()) != null) {
            if (line.trim().length() > 0) {
                String[] tokens = line.split("\\s");
                String id = tokens[0];
                String sample = tokens[1];
                map.addSeq(sample, id);
            }
        }
        return map;
    }

    public static void fromFile(File file, SampleMapping<String> incremental) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.trim().length() > 0) {
                String[] tokens = line.split("\\s");
                String id = tokens[0];
                String sample = tokens[1];
                incremental.addSeq(sample, id);
            }
        }
    }
}
