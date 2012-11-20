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

import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author Alexandre Vilcek (alexandre.vilcek@oracle.com)
 */
public class ConfigProperties {
    
    public final static String KV_HOST_PORT = "kv.host.port";
    public final static String KV_NAME = "kv.name";
    public final static String KV_MAJOR_KEYS_MAPPING = "kv.major.keys.mapping";
    public final static String KV_MINOR_KEYS_MAPPING = "kv.minor.keys.mapping";
    
    public static String getKVHostPort(Configuration c) {
        return c.get(KV_HOST_PORT);
    }
    
    public static String getKVName(Configuration c) {
        return c.get(KV_NAME);
    }
    
    public static String getKVMajorKeys(Configuration c) {
        return c.get(KV_MAJOR_KEYS_MAPPING);
    }
    
    public static String getKVMinorKeys(Configuration c) {
        return c.get(KV_MINOR_KEYS_MAPPING);
    }
    	
}
