package sqlite

import java.io.FileWriter
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.sql.DriverManager
import scala.io.Source
import scala.util.{Try, Using, Success, Failure}
import scala.collection.parallel.CollectionConverters._

case class Transaction(
  timestamp:     LocalDateTime,
  productName:   String,
  expiryDate:    LocalDate,
  quantity:      Int,
  unitPrice:     Double,
  channel:       String,
  paymentMethod: String
)

case class ProcessedTransaction(
  transaction:   Transaction,
  finalDiscount: Double,
  finalPrice:    Double 
)

object Main extends App {

  val timestampFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
  val BATCH_SIZE   = 10000

  def logEvent(writer: java.io.PrintWriter, level: String, message: String): Unit = {
    val timestamp = LocalDateTime.now().withNano(0).toString
    writer.println(s"$timestamp $level $message")
  }

  def parseTransaction(line: String): Option[Transaction] = {
    val cols = line.split(",")
    if (cols.length < 7) None
    else Try {
      Transaction(
        timestamp     = LocalDateTime.parse(cols(0).trim, timestampFmt),
        productName   = cols(1).trim,
        expiryDate    = LocalDate.parse(cols(2).trim),
        quantity      = cols(3).trim.toInt,
        unitPrice     = cols(4).trim.toDouble,
        channel       = cols(5).trim,
        paymentMethod = cols(6).trim
      )
    }.toOption
  }

 type Rule = (Transaction => Boolean, Transaction => Double)

  def expiryQualifier(tx: Transaction): Boolean = {
    val daysLeft = ChronoUnit.DAYS.between(tx.timestamp.toLocalDate, tx.expiryDate)
    daysLeft > 0 && daysLeft < 30
  }
  def expiryCalculator(tx: Transaction): Double = {
    val daysLeft = ChronoUnit.DAYS.between(tx.timestamp.toLocalDate, tx.expiryDate)
    30.0 - daysLeft
  }

  def cheeseQualifier(tx: Transaction): Boolean = tx.productName.toLowerCase.contains("cheese")
  def cheeseCalculator(tx: Transaction): Double = 10.0

  def wineQualifier(tx: Transaction): Boolean = tx.productName.toLowerCase.contains("wine")
  def wineCalculator(tx: Transaction): Double = 5.0

  def specialDayQualifier(tx: Transaction): Boolean = {
    val date = tx.timestamp.toLocalDate
    date.getMonthValue == 3 && date.getDayOfMonth == 23
  }
  def specialDayCalculator(tx: Transaction): Double = 50.0

  def quantityQualifier(tx: Transaction): Boolean = tx.quantity >= 6
  def quantityCalculator(tx: Transaction): Double = tx.quantity match {
    case q if q >= 6  && q <= 9  => 5.0
    case q if q >= 10 && q <= 14 => 7.0
    case q if q >= 15            => 10.0
    case _                       => 0.0
  }

  def appChannelQualifier(tx: Transaction): Boolean = tx.channel.equalsIgnoreCase("App")
  def appChannelCalculator(tx: Transaction): Double = math.ceil(tx.quantity / 5.0) * 5.0

  def visaQualifier(tx: Transaction): Boolean = tx.paymentMethod.equalsIgnoreCase("Visa")
  def visaCalculator(tx: Transaction): Double = 5.0


 val rulesRegistry: List[Rule] = List(
    (expiryQualifier, expiryCalculator),
    (cheeseQualifier, cheeseCalculator),
    (wineQualifier, wineCalculator),
    (specialDayQualifier, specialDayCalculator),
    (quantityQualifier, quantityCalculator),
    (appChannelQualifier, appChannelCalculator),
    (visaQualifier, visaCalculator)
  )


 def processTransaction(tx: Transaction): ProcessedTransaction = {
    
    val passedRules = rulesRegistry.filter { case (qualifierFunc, _) => qualifierFunc(tx) }

    val calculatedDiscounts = passedRules.map { case (_, calculatorFunc) => calculatorFunc(tx) }

    val top2 = calculatedDiscounts.sorted.reverse.take(2)

    val finalDiscount = if (top2.length == 2) {
      (top2(0) + top2(1)) / 2.0
    } else if (top2.length == 1) {
      top2(0)
    } else {
      0.0
    }

    val subtotal   = tx.quantity * tx.unitPrice
    val finalPrice = subtotal - subtotal * (finalDiscount / 100.0)

    ProcessedTransaction(tx, finalDiscount, finalPrice)
  }


  def createTableIfAbsent(conn: java.sql.Connection): Unit = {
    Using(conn.createStatement()) { stmt =>
      stmt.executeUpdate("""
        create table if not exists processed_transactions (
          timestamp      text,
          product_name   text,
          expiry_date    text,
          quantity       integer,
          unit_price     real,
          channel        text,
          payment_method text,
          final_discount real,
          final_price    real
        )
      """)
    }
  }

  def insertBatch(conn: java.sql.Connection, batch: Seq[ProcessedTransaction]): Unit = {
    val insertSQL = """
      insert into processed_transactions
        (timestamp, product_name, expiry_date, quantity, unit_price,
         channel, payment_method, final_discount, final_price)
      values (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    Using(conn.prepareStatement(insertSQL)) { pstmt =>
      batch.foreach { pt =>
        val tx = pt.transaction
        pstmt.setString(1, tx.timestamp.toString)
        pstmt.setString(2, tx.productName)
        pstmt.setString(3, tx.expiryDate.toString)
        pstmt.setInt   (4, tx.quantity)
        pstmt.setDouble(5, tx.unitPrice)
        pstmt.setString(6, tx.channel)
        pstmt.setString(7, tx.paymentMethod)
        pstmt.setDouble(8, pt.finalDiscount)
        pstmt.setDouble(9, pt.finalPrice)
        pstmt.addBatch()
      }
      pstmt.executeBatch()
    }
    conn.commit()
  }


 Using(new java.io.PrintWriter(new FileWriter("rules_engine.log", true))) { logWriter =>
    logEvent(logWriter, "INFO", "Rule engine started.")

    Using(DriverManager.getConnection("jdbc:sqlite:rules_engine.db")) { conn =>
      createTableIfAbsent(conn)
      conn.setAutoCommit(false) 

      val csvSource = Source.fromFile("TRX10M.csv")
      val lines = csvSource.getLines().drop(1)

      Try {
        val totalProcessed = lines.grouped(BATCH_SIZE).zipWithIndex.foldLeft(0) {
          case (runningTotal, (chunk, batchIndex)) =>
            val batchNumber = batchIndex + 1

            val processed = chunk.par
              .flatMap(parseTransaction)
              .map(processTransaction)
              .toList

            insertBatch(conn, processed)

            val newTotal = runningTotal + processed.length
            logEvent(logWriter, "INFO", s"Batch $batchNumber done — ${processed.length} rows, $newTotal total so far")
            
            newTotal 
        }

        logEvent(logWriter, "INFO", s"Finished. Total rows processed: $totalProcessed")
        println(s"Done! Processed $totalProcessed transactions.")

      } match {
        case Failure(e) =>
          conn.rollback()
          logEvent(logWriter, "ERROR", s"Pipeline failed: ${e.getMessage}")
          println(s"Error: ${e.getMessage}")
        case Success(_) =>
      }
      csvSource.close()

    } match {
      case Failure(e) => logEvent(logWriter, "ERROR", s"Database connection failed: ${e.getMessage}")
      case Success(_) =>
    }
  }
}