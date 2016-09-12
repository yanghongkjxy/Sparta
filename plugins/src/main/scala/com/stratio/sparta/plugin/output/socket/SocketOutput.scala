/*
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
package com.stratio.sparta.plugin.output.socket

import java.io.{Serializable => JSerializable}
import java.net.Socket

import com.stratio.sparta.sdk.{TableSchema, Output}
import org.apache.spark.sql.DataFrame
import com.stratio.sparta.sdk.ValidatingPropertyMap._

/**
  * {{{
  *   "output": {
  *     "name": "socket-out",
  *     "type": "Socket",
  *     "configuration": {
  *       "host": "10.0.0.1",
  *       "port": 12306,
  *       "delimiter": ","
  *     }
  *   }
  * }}}
  */
class SocketOutput(keyName: String,
  version: Option[Int],
  properties: Map[String, JSerializable],
  bcSchema: Seq[TableSchema])
  extends Output(keyName, version, properties, bcSchema) {

  require(properties.hasKey("host"), "host must be provided")
  require(properties.hasKey("port"), "port must be provided")

  val host      = properties.getString("host")
  val port      = properties.getInt("port")
  val delimiter = properties.getString("delimiter", ",")

  def upsert(dataFrame: DataFrame, options: Map[String, String]): Unit = {
    val socket = new Socket(host, port)

    try {
      val out = socket.getOutputStream
      dataFrame.foreach(r => out.write(r.mkString("", delimiter, "\n").getBytes))
    } finally {
      socket.close()
    }
  }
}
