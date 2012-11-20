/*
* This file is part of Hive KV Storage Handler
* Copyright 2012 Alexandre Vilcek (alexandre.vilcek@oracle.com)
*
*   Licensed under the Apache License, Version 2.0 (the "License");
*   you may not use this file except in compliance with the License.
*   You may obtain a copy of the License at
*
*       http://www.apache.org/licenses/LICENSE-2.0
*
*   Unless required by applicable law or agreed to in writing, software
*   distributed under the License is distributed on an "AS IS" BASIS,
*   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*   See the License for the specific language governing permissions and
*   limitations under the License.
*/

package org.vilcek.hive.kv;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import oracle.kv.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;

/**
 *
 * @author Alexandre Vilcek (alexandre.vilcek@oracle.com)
 */
public class KVHiveInputSplit extends FileSplit implements InputSplit {   
    private String kvStore;
    private String[] kvHelperHosts;
    private int kvPart;
    private Direction direction;
    private int batchSize;
    private Key parentKey;
    private KeyRange subRange;
    private Depth depth;
    private Consistency consistency;
    private long timeout;
    private TimeUnit timeoutUnit;
    private String[] locations = new String[0];
    
    private static final String[] EMPTY_ARRAY = new String[] {};
    
    public KVHiveInputSplit() {
        super((Path) null, 0, 0, EMPTY_ARRAY);
    }
    
    public KVHiveInputSplit(Path dummyPath) {
        super(dummyPath, 0, 0, EMPTY_ARRAY);
    }

    @Override
    public long getLength() {
        return 1;
    }

    @Override
    public String[] getLocations() throws IOException {
        return locations;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(kvHelperHosts.length);
        for (int i = 0; i < kvHelperHosts.length; i++) {
            Text.writeString(out, kvHelperHosts[i]);
        }
        Text.writeString(out, kvStore);
        Text.writeString(out, "" + kvPart);
        Text.writeString(out, (direction == null ? "" : direction.name()));
        out.writeInt(batchSize);
        writeBytes(out, (parentKey == null ? null : parentKey.toByteArray()));
        writeBytes(out, (subRange == null ? null : subRange.toByteArray()));
        Text.writeString(out, (depth == null ? "" : depth.name()));
        writeBytes(out, (consistency == null ?
                         null :
                         consistency.toByteArray()));
        out.writeLong(timeout);
        Text.writeString(out, (timeoutUnit == null ? "" : timeoutUnit.name()));
        out.writeInt(locations.length);
        for (int i = 0; i < locations.length; i++) {
            Text.writeString(out, locations[i]);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        int nHelperHosts = in.readInt();
        kvHelperHosts = new String[nHelperHosts];
        for (int i = 0; i < nHelperHosts; i++) {
            kvHelperHosts[i] = Text.readString(in);
        }

        kvStore = Text.readString(in);
        kvPart = Integer.parseInt(Text.readString(in));
        String dirStr = Text.readString(in);
        if (dirStr == null || dirStr.equals("")) {
            direction = Direction.FORWARD;
        } else {
            direction = Direction.valueOf(dirStr);
        }

        batchSize = in.readInt();

        byte[] pkBytes = readBytes(in);
        if (pkBytes == null) {
            parentKey = null;
        } else {
            parentKey = Key.fromByteArray(pkBytes);
        }

        byte[] srBytes = readBytes(in);
        if (srBytes == null) {
            subRange = null;
        } else {
            subRange = KeyRange.fromByteArray(srBytes);
        }

        String depthStr = Text.readString(in);
        if (depthStr == null || depthStr.equals("")) {
            depth = Depth.PARENT_AND_DESCENDANTS;
        } else {
            depth = Depth.valueOf(depthStr);
        }

        byte[] consBytes = readBytes(in);
        if (consBytes == null) {
            consistency = null;
        } else {
            consistency = Consistency.fromByteArray(consBytes);
        }

        timeout = in.readLong();

        String tuStr = Text.readString(in);
        if (tuStr == null || tuStr.equals("")) {
            timeoutUnit = null;
        } else {
            timeoutUnit = TimeUnit.valueOf(tuStr);
        }

        int len = in.readInt();
        locations = new String[len];
        for (int i = 0; i < len; i++) {
            locations[i] = Text.readString(in);
        }
    }
    
    private void writeBytes(DataOutput out, byte[] bytes) throws IOException {
        if (bytes == null) {
            out.writeInt(0);
            return;
        }
        out.writeInt(bytes.length);
        out.write(bytes);
    }
    
    private byte[] readBytes(DataInput in) throws IOException {
        int len = in.readInt();
        if (len == 0) {
            return null;
        }

        byte[] ret = new byte[len];
        in.readFully(ret);
        return ret;
    }
    
     KVHiveInputSplit setLocations(String[] locations) {
        this.locations = locations;
        return this;
    }

    KVHiveInputSplit setDirection(Direction direction) {
        this.direction = direction;
        return this;
    }

    Direction getDirection() {
        return direction;
    }

    KVHiveInputSplit setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    int getBatchSize() {
        return batchSize;
    }

    KVHiveInputSplit setParentKey(Key parentKey) {
        this.parentKey = parentKey;
        return this;
    }

    Key getParentKey() {
        return parentKey;
    }

    KVHiveInputSplit setSubRange(KeyRange subRange) {
        this.subRange = subRange;
        return this;
    }

    KeyRange getSubRange() {
        return subRange;
    }

    KVHiveInputSplit setDepth(Depth depth) {
        this.depth = depth;
        return this;
    }

    Depth getDepth() {
        return depth;
    }

    KVHiveInputSplit setConsistency(Consistency consistency) {
        this.consistency = consistency;
        return this;
    }

    Consistency getConsistency() {
        return consistency;
    }

    KVHiveInputSplit setTimeout(long timeout) {
        this.timeout = timeout;
        return this;
    }

    long getTimeout() {
        return timeout;
    }

    KVHiveInputSplit setTimeoutUnit(TimeUnit timeoutUnit) {
        this.timeoutUnit = timeoutUnit;
        return this;
    }

    TimeUnit getTimeoutUnit() {
        return timeoutUnit;
    }

    KVHiveInputSplit setKVHelperHosts(String[] kvHelperHosts) {
        this.kvHelperHosts = kvHelperHosts;
        return this;
    }

    String[] getKVHelperHosts() {
        return kvHelperHosts;
    }

    KVHiveInputSplit setKVStoreName(String kvStore) {
        this.kvStore = kvStore;
        return this;
    }

    String getKVStoreName() {
        return kvStore;
    }

    KVHiveInputSplit setKVPart(int kvPart) {
        this.kvPart = kvPart;
        return this;
    }

    int getKVPart() {
        return kvPart;
    }
    
}
