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

package com.stratio.sparkta.plugin.test.parser.shopping.center

import com.stratio.sparkta.plugin.parser.shopping.center.ShoppingCenterParser
import com.stratio.sparkta.sdk.{Event, Input}
import org.junit.runner.RunWith
import org.scalatest.WordSpecLike
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ShoppingCenterParserSpec extends WordSpecLike {

  val inputField = Input.RawDataKey
  val outputsFieldsOrder = Seq("timestamp", "day_time_zone")
  val outputsFieldsOrderLine = outputsFieldsOrder ++ Seq("product", "family", "quantity", "price")

  "A ShoppingCenterParser" should {
    "parse an order" in {
      val myJson1 =
        """{"operation":"insert","streamName":"c_lines","session_id":"1441263379141","request_id":"1441265665815","request":"","timestamp":1441265665815,"columns":[{"column":"line_id","value":"57ca1e7c-6f3e-4eb0-a34a-b74c1acab6f9"},{"column":"order_id","value":"a568f1b1-d7b5-49b0-9b2d-c8219a89556f"},{"column":"day_time_zone","value":"morning"},{"column":"client_id","value":"996"},{"column":"payment_method","value":"online"},{"column":"latitude","value":"34.7178"},{"column":"longitude","value":"10.6908"},{"column":"credit_card","value":"8193620556034818"},{"column":"shopping_center","value":"Valencia"},{"column":"channel","value":"OFFLINE"},{"column":"city","value":"Valencia"},{"column":"country","value":"SPAIN"},{"column":"employee","value":"245"},{"column":"total_amount","value":"10863.7"},{"column":"total_products","value":"29"},{"column":"order_size","value":"BIG"},{"column":"product","value":"adidas sneakers"},{"column":"family","value":"clothes"},{"column":"quantity","value":"6"},{"column":"price","value":"80.0"}],"userDefined":true}
          |""".stripMargin

      val e1 = new Event(Map(Input.RawDataKey -> myJson1))

      val result = new ShoppingCenterParser("name", 1, inputField, outputsFieldsOrder, Map()).parse(e1)

      assertResult(true)(result.keyMap.contains("timestamp"))
      assertResult(false)(result.keyMap.contains("family"))
    }

    "parse a line order" in {
      val myJson1 =
        """{"operation":"insert","streamName":"c_orders","session_id":"1441090777890","request_id":"1441090837177",
          |"request":"","timestamp":1441198159913,"columns":[{"column":"order_id","value":"00000000-0000-0000-C000-000000000046"},{"column":"day_time_zone","value":"afternoon"},{"column":"client_id","value":"7"},{"column":"payment_method","value":"credit card"},{"column":"latitude","value":"5.34644"},{"column":"longitude","value":"-74.49147"},{"column":"credit_card","value":"4444333322221111"},{"column":"shopping_center","value":"Barcelona"},{"column":"channel","value":"OFFLINE"},{"column":"city","value":"Barcelona"},{"column":"country","value":"SPAIN"},{"column":"employee","value":"48"},{"column":"total_amount","value":"2880.93"},{"column":"total_products","value":"3"},{"column":"order_size","value":"BIG"},{"column":"lines","value":"[{\"product\":\"PEANUTS\",\"family\":\"Feeding\",\"quantity\":3,\"price\":46.03}]"}],"userDefined":true}""".stripMargin


      val e1 = new Event(Map(Input.RawDataKey -> myJson1))

      val result = new ShoppingCenterParser("name", 1, inputField, outputsFieldsOrderLine, Map()).parse(e1)

      assertResult(true)(result.keyMap.contains("timestamp"))
      assertResult(true)(result.keyMap.contains("quantity"))
      assertResult(true)(result.keyMap.contains("price"))
      assertResult(true)(result.keyMap.contains("family"))
      assertResult(true)(result.keyMap.contains("product"))
    }
  }
}
