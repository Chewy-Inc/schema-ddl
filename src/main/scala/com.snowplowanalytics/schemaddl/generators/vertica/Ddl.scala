/*
 * Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.schemaddl
package generators
package vertica

// Scalaz
import scalaz._
import Scalaz._

object Ddl {

  // Base classes

  /**
   * Base class for everything that can be represented as Redshift DDL
   */
  abstract sealed trait Ddl {
    /**
     * Output actual DDL as string
     *
     * @return valid DDL
     */
    def toDdl: String

    /**
     * Aggregates all warnings from child elements
     */
    val warnings: List[String] = Nil

    /**
     * Append specified amount of ``spaces`` to the string to produce formatted DDL
     *
     * @param spaces amount of spaces
     * @param str string itself
     * @return string with spaces
     */
    def withTabs(spaces: Int, str: String) =
      if (str.length == 0) " " * (spaces)
      else if (spaces <= str.length) str
      else str + (" " * (spaces - str.length))
  }

  // CREATE TABLE

  abstract sealed trait DataType extends Ddl
  abstract sealed trait ColumnAttribute extends Ddl
  abstract sealed trait ColumnConstraint extends Ddl
  abstract sealed trait TableConstraint extends Ddl
  abstract sealed trait CompressionEncoding extends ColumnAttribute
  abstract sealed trait TableAttribute extends Ddl

  /**
   * Class holding all information about Redshift's column
   *
   * @param columnName column_name
   * @param dataType data_type such as INTEGER, VARCHAR, etc
   * @param columnAttributes set of column_attributes such as ENCODE
   * @param columnConstraints set of column_constraints such as NOT NULL
   */
  case class Column(columnName: String,
                    dataType: DataType,
                    columnAttributes: Set[ColumnAttribute] = Set.empty[ColumnAttribute],
                    columnConstraints: Set[ColumnConstraint] = Set.empty[ColumnConstraint]) extends Ddl {

    /**
     * Formatted column's DDL
     * Calling method must provide length for each tab via Tuple5
     *
     * @param tabs tuple of lengths (prepend, table_name, data_type, etc)
     * @return formatted DDL
     */
    def toFormattedDdl(tabs: (Int, Int, Int, Int, Int)) =
      withTabs(tabs._1, " ") +
      withTabs(tabs._2, nameDdl) +
      withTabs(tabs._3, dataTypeDdl) +
      //withTabs(tabs._4, attributesDdl) +
      //withTabs(tabs._5, constraintsDdl)
      withTabs(tabs._4, constraintsDdl) +
      withTabs(tabs._5, attributesDdl)

    /**
     * Compact way to output column
     *
     * @return string representing column without formatting
     */
    def toDdl = toFormattedDdl((1, 1, 1, 1, 1))

    // Get warnings only from data types suggestions
    override val warnings = dataType.warnings

    /**
     * column_name ready to output with surrounding quotes to prevent odd chars
     * from breaking the table
     */
    //val nameDdl = "\"" + columnName + "\" "
    //val nameDdl =  columnName + " "
    val nameDdl =  columnName.replace('.', '_') + " "

    /**
     * data_type ready to output
     */
    val dataTypeDdl = dataType.toDdl
  
    /**
     * column_constraints ready to output if exists
     */
    val constraintsDdl = columnConstraints.map(" " + _.toDdl).mkString(" ")

    /**
     * column_attributes ready to output if exists
     */
    val attributesDdl = columnAttributes.map(" " + _.toDdl).mkString(" ")

  }

  /**
   * Class holding all information about Redshift's table
   *
   * @param tableName table_name
   * @param columns iterable of all columns DDLs
   * @param tableConstraints set of table_constraints such as PRIMARY KEY
   * @param tableAttributes set of table_attributes such as DISTSTYLE
   */
  case class Table(tableName: String,
                   columns: List[Column],
                   tableConstraints: Set[TableConstraint] = Set.empty[TableConstraint],
                   tableAttributes: Set[TableAttribute] = Set.empty[TableAttribute]) extends Ddl {
    def toDdl = {
      val columnsDdl = columns.map(_.toFormattedDdl(tabulation)
                              .replaceAll("\\s+$", ""))
                              .mkString(",\n")
      s"""CREATE TABLE IF NOT EXISTS $tableName (
         |$columnsDdl$getConstraints
         |)$getAttributes;""".stripMargin
    }

    // Collect warnings from every column
    override val warnings = columns.flatMap(_.warnings)

    // Tuple with lengths of each column in formatted DDL file
    private val tabulation = {
      def getLength(f: Column => Int): Int =
        columns.foldLeft(0)((acc, b) => if (acc > f(b)) acc else f(b))

      val prepend = 4
      val first = getLength(_.nameDdl.length)
      val second = getLength(_.dataType.toDdl.length)
      val third = getLength(_.attributesDdl.length)
      val fourth = getLength(_.constraintsDdl.length)

      (prepend, first, second, third, fourth)
    }

    /**
     * Format constraints for table
     *
     * @return string with formatted table_constaints
     */
    private def getConstraints: String = {
      if (tableConstraints.isEmpty) ""
      else ",\n" + tableConstraints.map(c => withTabs(tabulation._1, " ") + c.toDdl).mkString("\n")
    }
    /**
     * Format attributes for table
     *
     * @return string with formatted table_attributes
     */
    private def getAttributes: String = {
      if (tableConstraints.isEmpty) ""
      else "\n" + tableAttributes.map(_.toDdl).mkString("\n")
    }
  }

  /**
   * Reference table. Used in foreign key and table constraint
   *
   * @param reftable name of table
   * @param refcolumn optional column
   */
  case class RefTable(reftable: String, refcolumn: Option[String]) extends Ddl {
    def toDdl = {
      val column = refcolumn.map("(" + _ + ")").getOrElse("")
      s"REFERENCES $reftable$column"
    }
  }

  /**
   * Data types
   * http://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html
   */
  object DataTypes {
    case object RedshiftTimestamp extends DataType { def toDdl = "TIMESTAMP".toLowerCase() }

    case object RedshiftSmallInt extends DataType { def toDdl = "SMALLINT".toLowerCase() }

    case object RedshiftInteger extends DataType { def toDdl = "INT".toLowerCase() }

    case object RedshiftBigInt extends DataType { def toDdl = "BIGINT".toLowerCase() }

    case object RedshiftReal extends DataType { def toDdl = "REAL".toLowerCase() }

    case object RedshiftDouble extends DataType { def toDdl = "DOUBLE PRECISION".toLowerCase() }

    case class RedshiftDecimal(precision: Option[Int], scale: Option[Int]) extends DataType {
      def toDdl = (precision, scale) match {
        case (Some(p), Some(s)) => s"DECIMAL ($p, $s)".toLowerCase()
        case _ => "DECIMAL".toLowerCase()
      }
    }

    case object RedshiftBoolean extends DataType { def toDdl = "BOOLEAN".toLowerCase()}

    case class RedshiftVarchar(size: Int) extends DataType { def toDdl = s"VARCHAR($size)".toLowerCase() }

    case class RedshiftChar(size: Int) extends DataType { def toDdl = s"CHAR($size)".toLowerCase()}
  }

  /**
   * These predefined data types assembles into usual Redshift data types, but
   * can store additional information such as warnings.
   * Using to prevent output on DDL-generation step.
   */
  object CustomDataTypes {
    case class ProductType(override val warnings: List[String]) extends DataType { def toDdl = "VARCHAR(4096)".toLowerCase() }
  }

  /**
   * column_attributes are:
   * [ DEFAULT default_expr ]
   * [ IDENTITY ( seed, step ) ]
   * [ ENCODE encoding ]
   * [ DISTKEY ]
   * [ SORTKEY ]
   */
  object ColumnAttributes {
    case class Default(value: String) extends ColumnAttribute { def toDdl = s"DEFAULT $value".toLowerCase() }

    case class Identity(seed: Int, step: Int) extends ColumnAttribute { def toDdl = s"IDENTITY ($seed, $step)".toLowerCase() }

    case class Encode(encoding: CompressionEncoding) extends ColumnAttribute { def toDdl = s"ENCODE ${encoding.toDdl}".toLowerCase() }

    case object DistKey extends ColumnAttribute { def toDdl = "SEGMENTED BY hash" }

    case object SortKey extends ColumnAttribute { def toDdl = "ORDER BY" }
  }

  /**
   * column_constraints are:
   * [ { NOT NULL | NULL } ]
   * [ { UNIQUE  |  PRIMARY KEY } ]
   * [ REFERENCES reftable [ ( refcolumn ) ] ]
   */
  object ColumnConstraints {
    abstract sealed trait Nullability extends ColumnConstraint
    case object Null extends Nullability { def toDdl = "NULL".toLowerCase() }
    case object NotNull extends Nullability { def toDdl = "NOT NULL".toLowerCase() }

    abstract sealed trait KeyConstraint extends ColumnConstraint
    case object Unique extends KeyConstraint { def toDdl = "UNIQUE".toLowerCase() }
    case object PrimaryKey extends KeyConstraint { def toDdl = "PRIMARY KEY".toLowerCase() }
  }

  /**
   * table_constraints  are:
   * [ UNIQUE ( column_name [, ... ] ) ]
   * [ PRIMARY KEY ( column_name [, ... ] )  ]
   * [ FOREIGN KEY (column_name [, ... ] ) REFERENCES reftable [ ( refcolumn ) ]
   */
  object TableConstraints {
    case class UniqueKey(columns: NonEmptyList[String]) extends TableConstraint {
      def toDdl = s"UNIQUE (${columns.list.map(_.mkString(", "))})"
    }

    case class PrimaryKey(columns: NonEmptyList[String]) extends TableConstraint {
      def toDdl = s"PRIMARY KEY (${columns.list.map(_.mkString(", "))})"
    }

    case class ForeignKey(columns: NonEmptyList[String], reftable: RefTable) extends TableConstraint {
      def toDdl = s"FOREIGN KEY (${columns.list.mkString(",")}) ${reftable.toDdl}"
    }
  }

  /**
   * table_attributes are:
   * [ DISTSTYLE { EVEN | KEY | ALL } ]
   * [ DISTKEY ( column_name ) ]
   * [ [COMPOUND | INTERLEAVED ] SORTKEY ( column_name [, ...] ) ]
   */
  object TableAttributes {
    abstract sealed trait DiststyleValue extends Ddl
    //case object Even extends DiststyleValue { def toDdl = "EVEN" }
    //case object Key extends DiststyleValue { def toDdl = "KEY" }
    //case object All extends DiststyleValue { def toDdl = "ALL" }
    case object Even extends DiststyleValue { def toDdl = "" }
    case object Key extends DiststyleValue { def toDdl = "" }
    case object All extends DiststyleValue { def toDdl = "" }

    abstract sealed trait Sortstyle extends Ddl
    //case object CompoundSortstyle extends Sortstyle { def toDdl = "COMPOUND" }
    //case object InterleavedSortstyle extends Sortstyle { def toDdl = "INTERLEAVED" }
    case object CompoundSortstyle extends Sortstyle { def toDdl = "" }
    case object InterleavedSortstyle extends Sortstyle { def toDdl = "" }


    case class Diststyle(diststyle: DiststyleValue) extends TableAttribute {
      //def toDdl = "DISTSTYLE " + diststyle.toDdl
      def toDdl = ""
    }

    case class Sortkey(sortstyle: Option[Sortstyle], columns: NonEmptyList[String]) extends TableAttribute {
      //def toDdl = sortstyle.map(_.toDdl + " ").getOrElse("") + "SORTKEY (" + columns.list.mkString(",") + ")"
      def toDdl = sortstyle.map(_.toDdl + " ").getOrElse("") + "ORDER BY " + columns.list.mkString(",")
    }

    case class Distkey(columnName: String) extends TableAttribute {
      //def toDdl = s"DISTKEY ($columnName)"
      def toDdl = s"SEGMENTED BY \n    hash($columnName) ALL NODES\n"
    }

  }

  /**
   * Compression encodings
   * http://docs.aws.amazon.com/redshift/latest/dg/c_Compression_encodings.html
   */
  object CompressionEncodings {
    case object RawEncoding extends CompressionEncoding { def toDdl = "ENCODING AUTO".toLowerCase() }

    case object ByteDictEncoding extends CompressionEncoding { def toDdl = "ENCODING BLOCK_DICT".toLowerCase() }

    case object DeltaEncoding extends CompressionEncoding { def toDdl = "ENCODING DELTAVAL".toLowerCase() }

    case object Delta32kEncoding extends CompressionEncoding { def toDdl = "ENCODING DELTARANGE_COMP".toLowerCase() }

    case object LzoEncoding extends CompressionEncoding { def toDdl = "ENCODING GZIP_COMP".toLowerCase() }

    case object Mostly8Encoding extends CompressionEncoding { def toDdl = "ENCODING BZIP_COMP".toLowerCase() }

    case object Mostly16Encoding extends CompressionEncoding { def toDdl = "ENCODING BZIP_COMP".toLowerCase() }

    case object Mostly32Encoding extends CompressionEncoding { def toDdl = "ENCODING BZIP_COMP".toLowerCase() }

    case object RunLengthEncoding extends CompressionEncoding{ def toDdl = "ENCODING RLE".toLowerCase() }

    case object Text255Encoding extends CompressionEncoding{ def toDdl = "ENCODING BZIP_COMP".toLowerCase() }

    case object Text32KEncoding extends CompressionEncoding{ def toDdl = "ENCODING BZIP_COMP".toLowerCase() }
  }

  // CREATE SCHEMA

  case class Schema(schemaName: String) extends Ddl {
    def toDdl = s"CREATE SCHEMA IF NOT EXISTS $schemaName;"
  }

  // COMMENT

  /**
   * COMMENT ON
   * { TABLE object_name | COLUMN object_name.column_name |
   * CONSTRAINT constraint_name ON table_name |
   * DATABASE object_name |
   * VIEW object_name }
   * IS 'text'
   */
  case class Comment(tableName: String, comment: String) extends Ddl {
    def toDdl = s"COMMENT ON TABLE $tableName IS '$comment';"
  }
}

