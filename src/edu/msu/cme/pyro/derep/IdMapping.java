package edu.msu.cme.pyro.derep;

/*
 * Ribosomal Database Project II
 * Copyright 2009 Michigan State University Board of Trustees
 */


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author farrisry
 */
public class IdMapping<E> {

    private final Map<E, List<String>> idToIds = new HashMap<E, List<String>>();
    private final Set<String> seenSids = new HashSet();

    public List<String> getIds(E id) {
        return Collections.unmodifiableList(idToIds.get(id));
    }

    public Set<E> getAll() {
        return Collections.unmodifiableSet(idToIds.keySet());
    }

    public Map<String, E> getReverseMapping() {
        Map<String, E> revMap = new HashMap<String,E>();
        for (E key : idToIds.keySet()) {
            for (String id : idToIds.get(key)) {
                revMap.put(id, key);
            }
        }
        return revMap;
    }

    public void addId(E exemplarId, String id) {
        List<String> ids = idToIds.get(exemplarId);
        if (ids == null) {
            ids = new ArrayList<String>();
            idToIds.put(exemplarId, ids);
        }

        if(seenSids.contains(id)) {
            throw new IllegalArgumentException(id + " is already mapped");
        }
        seenSids.add(id);

        ids.add(id);
    }

    public void addIds(E exemplarId, List<String> idsToAdd) {
        List<String> ids = idToIds.get(exemplarId);
        if (ids == null) {
            ids = new ArrayList<String>();
            idToIds.put(exemplarId, ids);
        }

        if(!Collections.disjoint(seenSids, idsToAdd)) {
            seenSids.retainAll(idsToAdd);
            throw new IllegalArgumentException("Attempting to add duplicate ids " + seenSids);
        }

        seenSids.addAll(idsToAdd);
        ids.addAll(idsToAdd);
    }

    public static IdMapping<Integer> fromFile(File mapFile) {
        BufferedReader reader = null;
        try {
            IdMapping<Integer> map = new IdMapping<Integer>();
            reader = new BufferedReader(new FileReader(mapFile));
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().length() != 0) {
                    StringTokenizer st = new StringTokenizer(line);
                    Integer id = new Integer(st.nextToken());
                    List<String> ids = new ArrayList<String>();

                    if(map.idToIds.containsKey(id)) {
                        throw new IOException("ID " + id + " appears multiple times in the id mapping");
                    }

                    map.idToIds.put(id, ids);

                    String[] sids = st.nextToken().split(",");
                    for (String sid: sids) {
                        ids.add(sid);
                        if(map.seenSids.contains(sid)) {
                            throw new IOException("SID " + sid + " appears multiple times in the id mapping");
                        }

                        map.seenSids.add(sid);
                    }
                }
            }
            return map;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } finally {
            try {
                reader.close();
            } catch (IOException ex) {
            }
        }
    }

    public int size() {
        return idToIds.size();
    }

    public void toStream(PrintStream out) {
        for (E id : idToIds.keySet()) {
            List<String> ids = idToIds.get(id);
            StringBuilder buf = new StringBuilder();
            buf.append(id).append(' ');
            boolean previous = false;
            for (String sid : ids) {
                if (previous) {
                    buf.append(',');
                }
                previous = true;
                buf.append(sid);
            }
            out.println(buf.toString());
        }
        out.flush();
        out.close();
    }
}
