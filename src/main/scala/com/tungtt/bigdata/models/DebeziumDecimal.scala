package com.tungtt.bigdata.models

import java.math.BigInteger

case class DebeziumDecimal(var scale: Int, var value: String) {

  def toDecimal: BigDecimal = {
    BigDecimal(new BigInteger(value.getBytes()), scale)
  }
}
