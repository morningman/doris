// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.http.config;

import org.apache.doris.PaloFe;
import org.apache.doris.common.Log4jConfig;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

public class SpringLog4j2Config {

    /**
     * write spring boot log4j2-spring.xml file
     */
    public static void writeSpringLogConf() throws IOException {
        Writer writer = null;
        try {
            //log4j2-spring.xml file path
            String logConfFilePath = PaloFe.class.getResource("/").getPath();
            File file = new File(logConfFilePath + "log4j2-spring.xml");
            if(!file.exists()){
                file.createNewFile();
                //write file
                writer = new FileWriter(file);
                writer.write(Log4jConfig.getLogXmlConfTemplate());
            } else {
                file.deleteOnExit();
                file.createNewFile();
                //write file
                writer = new FileWriter(file);
                writer.write(Log4jConfig.getLogXmlConfTemplate());
            }
            System.out.println("==============================");
        }catch (IOException e){
            e.printStackTrace();
        } finally {
            if(writer != null){
                writer.close();
            }
        }
    }
}
