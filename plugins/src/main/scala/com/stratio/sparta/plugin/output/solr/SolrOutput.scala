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
package com.stratio.sparta.plugin.output.solr

import java.io.{Serializable => JSerializable}
import scala.util.Try

import org.apache.solr.client.solrj.SolrServer
import org.apache.spark.sql._

import com.stratio.sparta.sdk.Output._
import com.stratio.sparta.sdk._

//import scala.collection._
import com.stratio.sparta.sdk.ValidatingPropertyMap._

class SolrOutput(keyName: String,
                 version: Option[Int],
                 properties: Map[String, Serializable],
                 schemas: Seq[TableSchema])
  extends Output(keyName, version, properties, schemas) with SolrDAO {

  override val idField = properties.getString("idField", None)

  override val connection = properties.getString("connection", s"$DefaultNode:$DefaultPort")

  override val createSchema = Try(properties.getString("createSchema").toBoolean).getOrElse(true)

  override val isCloud = Try(properties.getString("isCloud").toBoolean).getOrElse(true)

  override val dataDir = properties.getString("dataDir", None)

  override val tokenizedFields = Try(properties.getString("tokenizedFields").toBoolean).getOrElse(false)

  @transient
  private val solrClients: Map[String, SolrServer] = {
    schemas.map(tschemaFiltered =>
      tschemaFiltered.tableName -> getSolrServer(connection, isCloud)).toMap
  }

  override def setup(options: Map[String, String]): Unit = {
    if (validConfiguration) createCores else log.info(SolrConfigurationError)
  }

  private def createCores: Unit = {
    val coreList = getCoreList(connection, isCloud)
    schemas.filter(tschema => tschema.outputs.contains(keyName)).foreach(tschemaFiltered => {
      if (!coreList.contains(tschemaFiltered.tableName)) {
        createCoreAccordingToSchema(solrClients, tschemaFiltered.tableName, tschemaFiltered.schema)
      }
    })
  }

  //scalastyle:off
  override def upsert(dataFrame: DataFrame, options: Map[String, String]): Unit = {
    val tableName = getTableNameFromOptions(options)
    val overwrite = true
    val skipDefaultIndex = true
    SolrOutputWriter.insert(dataFrame, overwrite, connection, skipDefaultIndex, tableName)
  }

}