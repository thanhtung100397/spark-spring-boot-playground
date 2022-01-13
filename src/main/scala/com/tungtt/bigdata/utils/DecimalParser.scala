package com.tungtt.bigdata.utils

import java.math.BigInteger

object DecimalParser {
  def parse(scale: Int, value: String) : BigDecimal = {
    BigDecimal(new BigInteger(value.getBytes()), scale)
  }
}
