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
package com.stratio.sparta.serving.api

import java.util.UUID

import akka.event.slf4j.SLF4JLogging
import org.apache.log4j.BasicConfigurator
import org.json4s.jackson.Serialization._
import com.stratio.sparta.driver.SpartaJob
import com.stratio.sparta.driver.factory.SparkContextFactory
import com.stratio.sparta.serving.api.actor.ClusterLauncherActor
import com.stratio.sparta.serving.api.constants.ActorsConstant
import com.stratio.sparta.serving.api.helpers.SpartaHelper
import com.stratio.sparta.serving.core.{CuratorFactoryHolder, SpartaConfig}
import com.stratio.sparta.serving.core.constants.AppConstant
import com.stratio.sparta.serving.core.models.{SpartaSerializer, AggregationPoliciesModel}
import org.apache.spark.{SparkContext, SparkConf}
import org.json4s.native.Serialization.{read, write}

/**
 * Entry point of the application.
 */
object Sparta extends App with SLF4JLogging with SpartaSerializer {

  BasicConfigurator.configure();
//  val string = scala.io.Source.fromFile("/home/anistal/Downloads/websocket-to-mongo.json").mkString

  SpartaConfig.initMainConfig()
  SpartaConfig.initApiConfig()

  val string = scala.io.Source.fromFile("/tmp/policy.json").mkString
  val policy = read[AggregationPoliciesModel](string)

  // In local
//  val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
//  val sc = new SparkContext(conf)
//  SparkContextFactory.setSparkContext(sc)
//  val spartaJob = new SpartaJob(policy)
//  spartaJob.run(sc)

  //  In cluster
  val policyS = if (policy.id == None) {
    policy.copy(id = Some(s"${UUID.randomUUID.toString}"),
      name = policy.name.toLowerCase,
      version = Some(ActorsConstant.UnitVersion))
  } else {
    policy
  }

  val curatorFramework = CuratorFactoryHolder.getInstance()
  curatorFramework.create().creatingParentsIfNeeded().forPath(
    s"${AppConstant.PoliciesBasePath}/${policyS.id.get}", write(policyS).getBytes)

  //scalastyle:off
  new ClusterLauncherActor(policyS, null).doInitSpartaContext()

  log.info("Waiting 120 seconds after submitting the job...")
  Thread.sleep(120 * 1000)
}
