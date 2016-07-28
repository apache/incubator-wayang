package org.qcri.rheem.apps.tpch.queries

import org.qcri.rheem.api._
import org.qcri.rheem.apps.tpch.CsvUtils
import org.qcri.rheem.apps.tpch.data.LineItem
import org.qcri.rheem.core.api.{Configuration, RheemContext}
import org.qcri.rheem.core.platform.Platform
import org.qcri.rheem.sqlite3.operators.Sqlite3TableSource

/**
  * Rheem implementation of TPC-H Query 3.
  *
  * {{{
  * select
  *   l_orderkey,
  *   sum(l_extendedprice*(1-l_discount)) as revenue,
  *   o_orderdate,
  *   o_shippriority
  * from
  *   customer,
  *   orders,
  *   lineitem
  * where
  *   c_mktsegment = '[SEGMENT]'
  *   and c_custkey = o_custkey
  *   and l_orderkey = o_orderkey
  *   and o_orderdate < date '[DATE]'
  *   and l_shipdate > date '[DATE]'
  * group by
  *   l_orderkey,
  *   o_orderdate,
  *   o_shippriority
  * order by
  *   revenue desc,
  *   o_orderdate;
  * }}}
  */
class Query3Hybrid(platforms: Platform*) {

  def apply(configuration: Configuration,
            segment: String = "BUILDING",
            date: String = "1995-03-15") = {

    val rheemCtx = new RheemContext(configuration)
    platforms.foreach(rheemCtx.register)

    val lineitemFile = configuration.getStringProperty("rheem.apps.tpch.csv.lineitem")

    // Read, filter, and project the customer data.
    val _segment = segment
    val customerKeys = rheemCtx
      .load(new Sqlite3TableSource("CUSTOMER"))
      .withName("Load CUSTOMER table")

      .filter(_.getString(6) == _segment, sqlUdf = s"c_mktsegment LIKE '$segment%'", selectivity = .25)
      .withName("Filter customers")

      .map(_.getLong(0)) // TODO: Cannot express projection across SQL/Java
      .withName("Project customers")

    // Read, filter, and project the order data.
    val _date = CsvUtils.parseDate(date)
    val orders = rheemCtx
      .load(new Sqlite3TableSource("ORDERS"))
      .withName("Load ORDERS table")

      .filter(t => CsvUtils.parseDate(t.getString(4)) > _date, sqlUdf = s"o_orderdate < date('$date')")
      .withName("Filter orders")

      .map(order => (order.getLong(0), // orderKey
        order.getLong(1), // custKey
        CsvUtils.parseDate(order.getString(4)), // orderDate
        order.getInt(7)) // shipPriority
      ) // TODO: Cannot express projection across SQL/Java
      .withName("Project orders")

    // Read, filter, and project the line item data.
    val lineItems = rheemCtx
      .readTextFile(lineitemFile)
      .withName("Read line items")
      .map(LineItem.parseCsv)
      .withName("Parse line items")

      .filter(_.shipDate > _date)
      .withName("Filter line items")

      .map(li => (li.orderKey, li.extendedPrice * (1 - li.discount)))
      .withName("Project line items")

    // Join and aggregate the different datasets.
    customerKeys
      .join[(Long, Long, Int, Int), Long](identity, orders, _._2)
      .withName("Join customers with orders")
      .map(_.field1) // (orderKey, custKey, orderDate, shipPriority)
      .withName("Project customer-order join product")

      .join[(Long, Double), Long](_._1, lineItems, _._1)
      .withName("Join CO with line items")
      .map(coli => Query3Result(
        orderKey = coli.field1._1,
        revenue = coli.field1._2,
        orderDate = coli.field0._3,
        shipPriority = coli.field0._4
      ))
      .withName("Project CO-line-item join product")

      .reduceByKey(
        t => (t.orderKey, t.orderDate, t.shipPriority),
        (t1, t2) => {
          t1.revenue += t2.revenue;
          t2
        }
      )
      .withName("Aggregate revenue")
      .withUdfJarsOf(classOf[Query3Hybrid])
      .collect(s"TPC-H (${this.getClass.getSimpleName})")
  }

}
