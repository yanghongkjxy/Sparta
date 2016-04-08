/**
 * Copyright (C) 2015 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.launcher;

import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.spark.launcher.CommandBuilderUtils.isWindows;
import static org.apache.spark.launcher.CommandBuilderUtils.join;
import static org.apache.spark.launcher.CommandBuilderUtils.quoteForBatchScript;

/**
 * Created by dcarroza on 7/04/16.
 */
public class SpartaLauncher extends SparkLauncher {

    private static final Logger log = LoggerFactory.getLogger(SpartaLauncher.class);

    @Override
    public Process launch() throws IOException {
        List<String> cmd = new ArrayList<String>();
        String script = isWindows() ? "spark-submit.cmd" : "spark-submit";
        cmd.add(join(File.separator, builder.getSparkHome(), "bin", script));
        cmd.addAll(builder.buildSparkSubmitArgs());

        // Since the child process is a batch script, let's quote things so that special characters are
        // preserved, otherwise the batch interpreter will mess up the arguments. Batch scripts are
        // weird.
        if (isWindows()) {
            List<String> winCmd = new ArrayList<String>();
            for (String arg : cmd) {
                winCmd.add(quoteForBatchScript(arg));
            }
            cmd = winCmd;
        }

        StringBuffer command = new StringBuffer();
        for (String item : cmd) {
            command.append(" ").append(item);
        }
        log.info("Running command: " + command.toString());
        System.out.println("Running command: " + command.toString());

        ProcessBuilder pb = new ProcessBuilder(cmd.toArray(new String[cmd.size()]));
        for (Map.Entry<String, String> e : builder.childEnv.entrySet()) {
            pb.environment().put(e.getKey(), e.getValue());
        }
        return pb.start();
    }
}
