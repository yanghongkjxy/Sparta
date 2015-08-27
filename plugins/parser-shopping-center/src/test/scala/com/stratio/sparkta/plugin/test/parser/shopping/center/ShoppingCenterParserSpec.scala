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
        """{"timestamp":"2015-08-28 14:34:00","day_time_zone":"afternoon","client_id":481,
          |"payment_method":"creditcard","credit_card":9999888877776666,"shopping_center":"Valencia",
          |"channel":"ONLINE","city":"Madrid","country":"Spain","employee":27,"total_amount":1876.31,
          |"total_products":3,"order_size":"MEDIUM","lines":[{"product":"PEANUTS","family":"Feeding","quantity":1,
          |"price":25.72},{"product":"VINEGAR","family":"Feeding","quantity":2,"price":35.54},{"product":"MILK",
          |"family":"Feeding","quantity":3,"price":8.08}]}""".stripMargin

      val e1 = new Event(Map(Input.RawDataKey -> myJson1))

      val result = new ShoppingCenterParser("name", 1, inputField, outputsFieldsOrder, Map()).parse(e1)

      assertResult(true)(result.keyMap.contains("timestamp"))
      assertResult(false)(result.keyMap.contains("family"))
    }

    "parse a line order" in {
      val myJson1 =
        """{"timestamp":"2015-08-28 14:34:00","day_time_zone":"afternoon","client_id":481,
          |"payment_method":"creditcard","credit_card":9999888877776666,"shopping_center":"Valencia",
          |"channel":"ONLINE","city":"Madrid","country":"Spain","employee":27,"total_amount":1876.31,
          |"total_products":3,"order_size":"MEDIUM","lines":[{"product":"PEANUTS","family":"Feeding","quantity":1,
          |"price":25.72}]}""".stripMargin

      val e1 = new Event(Map(Input.RawDataKey -> myJson1))

      val result = new ShoppingCenterParser("name", 1, inputField, outputsFieldsOrderLine, Map()).parse(e1)

      assertResult(true)(result.keyMap.contains("timestamp"))
      assertResult(true)(result.keyMap.contains("family"))
    }
  }
}
