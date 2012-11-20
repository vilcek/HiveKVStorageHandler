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

import java.io.IOException;
import java.util.Properties;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.Progressable;

/**
 *
 * @author Alexandre Vilcek (alexandre.vilcek@oracle.com)
 */
public class KVHiveOutputFormat implements OutputFormat<NullWritable, MapWritable>, HiveOutputFormat<NullWritable, MapWritable> {

    @Override
    public RecordWriter getHiveRecordWriter(JobConf jc, Path path, Class<? extends Writable> type, boolean bln, Properties prprts, Progressable p) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public org.apache.hadoop.mapred.RecordWriter<NullWritable, MapWritable> getRecordWriter(FileSystem fs, JobConf jc, String string, Progressable p) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void checkOutputSpecs(FileSystem fs, JobConf jc) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}
