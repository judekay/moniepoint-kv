package kv.storage;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SsTableHandler {

    private final File file;
    private final List<File> sstableFiles = new ArrayList<>();

    public SsTableHandler(File file) {
        this.file = file;
        loadSsTable();
    }

    private void loadSsTable() {
        File[] files = file.listFiles(new  FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith("sstable_") && name.endsWith(".dat");
            }
        });

        if (files != null) {
            Arrays.sort(files);
            Collections.addAll(sstableFiles, files);
        }
    }

    public File newSsTableFile() {
        return new File(file.getParentFile(), file.getName() + ".sstable");
    }

    public List<File> getSsTableFile() {
        return Collections.unmodifiableList(sstableFiles);
    }

    public void registerSsTable(File file) {
        sstableFiles.add(file);
    }
}
