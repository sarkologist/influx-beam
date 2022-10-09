package util.influx

import InfluxSink.LOG
import io.razem.influxdbclient.{Database, InfluxDB}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

object InfluxSingleton {
  var db: Database       = _
  var influxdb: InfluxDB = _

  implicit lazy val ec: ExecutionContextExecutor =
    ExecutionContext.fromExecutor(
      // this is per jvm; so per dataflow worker
      // no reason why it is this specific value other than it works
      new java.util.concurrent.ForkJoinPool(2)
    )

  def getOrInitialise(host: String, port: Integer, database: String): Database =
    synchronized[Database] {
      if (db == null) {
        if (influxdb == null) {
          LOG.info("connecting to influx {} {} {}", host, port, database)
          influxdb = InfluxDB.connect(host, port)
        }

        db = influxdb.selectDatabase(database)

      }

      db
    }
}
