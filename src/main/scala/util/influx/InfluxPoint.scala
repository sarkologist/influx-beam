package util.influx

import io.razem.influxdbclient.Parameter.Precision
import io.razem.influxdbclient.{Field, Point, Tag}

object InfluxPoint {
  def mkPoint(measurement: String,
              timestamp: Long,
              precision: Precision.Precision,
              tags: Seq[Tag] = Nil,
              fields: Seq[Field] = Nil): InfluxPoint = {
    InfluxPoint(Point(measurement, timestamp, tags, fields),
                InfluxPrecision(precision))
  }
}
//wrapper for precision object, to avoid failure of getClass.getSimpleName for double nested inner class
//https://github.com/scala/bug/issues/2034
case class InfluxPrecision(precision: Precision.Precision)

case class InfluxPoint(p: Point, precision: InfluxPrecision) {
  def addTag(key: String, value: String) =
    copy(p = p.addTag(key, value))

  def addOptionalTag(key: String, value: String) = {
    if (value.nonEmpty) copy(p = p.addTag(key, value)) else this
  }

  def addField(key: String, value: String) =
    copy(p.addField(key, value))

  def addField(key: String, value: Double) =
    copy(p.addField(key, value))

  def addField(key: String, value: Long) =
    copy(p.addField(key, value))

  def addField(key: String, value: Boolean) =
    copy(p.addField(key, value))

  def addField(key: String, value: BigDecimal) =
    copy(p.addField(key, value))
}
