package com.tungtt.bigdata.models

case class SyncData[T](var payload: Payload[T]) {

}

case class Payload[T](var after: T) {

}