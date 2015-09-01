/**
 * Copyright (C) 2015 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stratio.sparkta.plugin.parser.shopping.center

import java.io.{Serializable => JSerializable}
import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

import akka.event.slf4j.SLF4JLogging

import scala.util.{Failure, Success, Try}
import scala.util.parsing.json.JSON

import com.stratio.sparkta.sdk.{Event, Parser}

class ShoppingCenterParser(name: String,
                           order: Integer,
                           inputField: String,
                           outputFields: Seq[String],
                           properties: Map[String, JSerializable])
  extends Parser(name, order, inputField, outputFields, properties) with SLF4JLogging {

  def addGeoTo(event: Map[String, JSerializable]): Map[String, JSerializable] = {
    val lat = event.get("lat") match {
      case Some(value) => if (value != Some(0.0)) Some(value.toString) else None
      case None => None
    }
    val lon = event.get("lon") match {
      case Some(value) => if (value != Some(0.0)) Some(value.toString) else None
      case None => None
    }
    val mapToReturn = (lat, lon) match {
      case (Some(latVal), Some(lonVal)) => "geo" -> Some(latVal + "__" + lonVal)
      case (None, None) => "geo" -> None
    }

    Map(mapToReturn)
  }

  def stringDimensionToDouble(dimensionName: String, newDimensionName: String, columnMap: Map[String, Any]):
  Map[String, JSerializable] = {
    columnMap.get(dimensionName) match {
      case Some(x: Double) => Map(newDimensionName -> x)
      case Some(x: String) => if (x == "") Map() else Map(newDimensionName -> x.toDouble)
      case Some(_) => Map(newDimensionName -> columnMap.get(dimensionName).getOrElse("0").toString.toDouble)
      case None => Map()
    }
  }

  def cloneDimension(dimensionName: String, newDimensionName: String, columnMap: Map[String, String]):
  Map[String, String] = {
    Map(newDimensionName -> columnMap.get(dimensionName).getOrElse("undefined"))
  }

  override def parse(data: Event): Event = {
    var event: Option[Event] = None
    data.keyMap.foreach(e => {
      if (inputField.equals(e._1)) {
        val result = e._2 match {
          case s: String => s
          case b: Array[Byte] => new String(b)
        }

        JSON.globalNumberParser = { input: String => input.toDouble }
        val json = JSON.parseFull(result)
        event = Some(new Event(json.get.asInstanceOf[Map[String, JSerializable]], Some(e._2)))
        val columns = event.get.keyMap.get("columns").get.asInstanceOf[List[Map[String, String]]]
        val columnMap = columns.map(c => c.get("column").get -> c.get("value").getOrElse("")).toMap

        val columnMapExtended = columnMap ++
          cloneDimension("order_id", "order_id", columnMap) ++
          cloneDimension("timestamp", "timestamp", columnMap) ++
          cloneDimension("day_time_zone", "day_time_zone", columnMap) ++
          cloneDimension("client_id", "client_id", columnMap) ++
          cloneDimension("credit_card", "credit_card", columnMap) ++
          cloneDimension("payment_method", "payment_method", columnMap) ++
          cloneDimension("shopping_center", "shopping_center", columnMap) ++
          cloneDimension("channel", "channel", columnMap) ++
          cloneDimension("city", "city", columnMap) ++
          cloneDimension("country", "country", columnMap) ++
          cloneDimension("employee", "employee", columnMap) ++
          cloneDimension("total_amount", "total_amount", columnMap) ++
          cloneDimension("total_products", "total_products", columnMap) ++
          cloneDimension("order_size", "order_size", columnMap)

//        val odometerMap = stringDimensionToDouble("odometer", "odometerNum", columnMapExtended)
//
//        val rmpAvgMap = stringDimensionToDouble("rpm_event_avg", "rpmAvgNum", columnMapExtended)

//        val resultMap = columnMapExtended ++ odometerMap ++ rmpAvgMap

        val resultMap = columnMapExtended

        event = Some(new Event((resultMap.asInstanceOf[Map[String, JSerializable]] ++ addGeoTo(resultMap))
          .filter(m => (m._2.toString != "") && outputFields.contains(m._1)), None))
      }
    })

    val parsedEvent = event match {
      case Some(x) => new Event(data.keyMap ++ x.keyMap)
      case None => data
    }
    parsedEvent
  }

}

