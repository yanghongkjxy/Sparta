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

  override def parse(data: Event): Event = {
    var event: Option[Event] = None
    data.keyMap.foreach(e => {
      if (inputField.equals(e._1)) {
        Try({
        val result = e._2 match {
          case s: String => s
          case b: Array[Byte] => new String(b)
        }

        JSON.globalNumberParser = { input: String => input.toDouble }
        val json = JSON.parseFull(result)

        if (json.isDefined){
          event = Some(new Event(json.get.asInstanceOf[Map[String, JSerializable]], Some(e._2)))

          val eventValuesMap = event.get.keyMap

          val resultMap =
            getStringDimensionFrom("order_id", eventValuesMap) ++
              getDateDimensionFrom("timestamp", eventValuesMap) ++
              getStringDimensionFrom("day_time_zone", eventValuesMap) ++
              getLongDimensionFrom("client_id", eventValuesMap) ++
              getStringDimensionFrom("payment_method", eventValuesMap) ++
              getStringDimensionFrom("credit_card", eventValuesMap) ++
              getStringDimensionFrom("shopping_center", eventValuesMap) ++
              getStringDimensionFrom("channel", eventValuesMap) ++
              getStringDimensionFrom("city", eventValuesMap) ++
              getStringDimensionFrom("country", eventValuesMap) ++
              getIntDimensionFrom("employee", eventValuesMap) ++
              getFloatDimensionFrom("total_amount", eventValuesMap) ++
              getIntDimensionFrom("total_products", eventValuesMap) ++
              getStringDimensionFrom("order_size", eventValuesMap) ++
              getFloatDimensionFrom("latitude", eventValuesMap) ++
              getFloatDimensionFrom("longitude", eventValuesMap) ++
              getLineOrderDimensions(eventValuesMap)

          event = Some(new Event((resultMap.asInstanceOf[Map[String, JSerializable]] ++ addGeoTo(resultMap))
            .filter(m => (m._2.toString != "") && outputFields.contains(m._1)), None))}
        })
        match {
          case Success(event) => event
          case Failure(e) => log.error("For event: " + data, e); None
        }
      }
    })
    event match {
      case Some(x) => new Event(data.keyMap ++ x.keyMap)
      case None => log.error("For event: " + data); new Event(data.keyMap)
    }
  }

  private def getLineOrderDimensions(eventValuesMap: Map[String, JSerializable]) = {
    if (isLineOrder(eventValuesMap)) {
      eventValuesMap.asInstanceOf[Map[String, Any]].get("lines")
        .get.asInstanceOf[List[Map[String, JSerializable]]].head
    } else {
      Map()
    }
  }

  def isLineOrder(eventValuesMap: Map[String, JSerializable]): Boolean = {
    eventValuesMap.asInstanceOf[Map[String, Any]].get("lines").get
      .asInstanceOf[List[Map[String, JSerializable]]].size == 1
  }

  def getDateDimensionFrom(dimensionName: String, eventValuesMap: Map[String, JSerializable]):
  Map[String, JSerializable] = {
    val format: DateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    Map(dimensionName -> format.parse(eventValuesMap.get(dimensionName).get.toString))
  }

  def getStringDimensionFrom(dimensionName: String, eventValuesMap: Map[String, JSerializable]):
  Map[String, JSerializable] = Map(dimensionName -> eventValuesMap.get(dimensionName).getOrElse(""))

  def getIntDimensionFrom(dimensionName: String, eventValuesMap: Map[String, JSerializable]):
  Map[String, JSerializable] = Map(dimensionName -> eventValuesMap.get(dimensionName).get.toString.
    replaceAll("\\.0*$", "").toInt)

  def getFloatDimensionFrom(dimensionName: String, eventValuesMap: Map[String, JSerializable]):
  Map[String, JSerializable] = Map(dimensionName -> eventValuesMap.get(dimensionName).get.toString.toFloat)

  def getLongDimensionFrom(dimensionName: String, eventValuesMap: Map[String, JSerializable]):
  Map[String, JSerializable] = Map(dimensionName -> eventValuesMap.get(dimensionName).get
    .toString.replaceAll("\\.0*$", "").toLong)

  def addGeoTo(event: Map[String, JSerializable]): Map[String, JSerializable] = {
    val lat = event.get("latitude") match {
      case Some(value) => if (value != Some(0.0)) Some(value.toString) else None
      case None => None
    }
    val lon = event.get("longitude") match {
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

}

