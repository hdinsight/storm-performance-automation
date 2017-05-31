/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.hdfs.bolt;

import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.task.TopologyContext;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.SimpleTimeZone;
import java.util.StringTokenizer;
import java.util.TimeZone;


/**
 * Rolls files every hour with the following format:
 * <pre>
 * 	yyyy/MM/dd/hh/<filename>
 * </pre>
 * UTC date time is used.
 *  
 * Creates file names with the following format:
 * <pre>
 *     {prefix}{componentId}-{taskId}-{rotationNum}-{timestamp}{extension}
 * </pre>
 * For example:
 * <pre>
 *     MyBolt-5-7-1390579837830.txt
 * </pre>
 *
 * By default, prefix is empty and extenstion is ".txt".
 *
 */
public class HourlyFileNameFormat implements FileNameFormat {

	private static final long serialVersionUID = -629169447311923341L;
	
	private String componentId;
    private int taskId;
    private String path = "/storm";
    private String prefix = "";
    private String extension = ".txt";

    public HourlyFileNameFormat withPrefix(String prefix){
        this.prefix = prefix;
        return this;
    }

    public HourlyFileNameFormat withExtension(String extension){
        this.extension = extension;
        return this;
    }

    public HourlyFileNameFormat withPath(String path){
        this.path = path;
        return this;
    }

    public void prepare(Map conf, TopologyContext topologyContext) {
        this.componentId = topologyContext.getThisComponentId();
        this.taskId = topologyContext.getThisTaskId();
    }
    
    public String getName(long rotation, long timeStamp) {
        return this.prefix + this.componentId + "-" + this.taskId +  "-" + rotation + "-" + timeStamp + this.extension;
    }

    public String getPath(){
    	DateFormat formatter = new SimpleDateFormat("yyyy MM dd HH");
	    formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
	    Date date= new Date();  
	    String dateString = formatter.format(date);
	    String[] dateValues = dateString.split("\\s");
	    return String.format("%s/%s/%s/%s/%s", this.path, dateValues[0], dateValues[1], dateValues[2], dateValues[3]);
    }
}