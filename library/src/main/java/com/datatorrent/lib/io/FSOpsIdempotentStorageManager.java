package com.datatorrent.lib.io;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;

import com.google.common.collect.Sets;

/**
 *
 */
public class FSOpsIdempotentStorageManager extends IdempotentStorageManager.FSIdempotentStorageManager {

    public Set<Integer> getOperatorIds() throws IOException
    {
        Set<Integer> ids = Sets.newLinkedHashSet();
        FileStatus[] fileStatuses = fs.listStatus(appPath);
        for (FileStatus fileStatus : fileStatuses) {
            ids.add(Integer.parseInt(fileStatus.getPath().getName()));
        }
        return ids;
    }

    public long[] getOrderedWindowIds(int operatorId) throws IOException {
        long[] windowIds = getWindowIds(operatorId);
        Arrays.sort(windowIds);
        return windowIds;
    }

}
