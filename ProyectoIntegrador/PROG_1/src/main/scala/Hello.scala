import slick.jdbc.OracleProfile.api._
import scala.concurrent.Await
import scala.concurrent.duration.Duration

import scala.concurrent.ExecutionContext.Implicits.global

case class Author(id: Int, name: String)
case class Book(id: Int, name: String)
case class AuthorBook(authorId: Int, bookId: Int)

object Hello extends App {
  class Authors(tag: Tag) extends Table[Author](tag, "author") {
    def id = column[Int]("id", O.PrimaryKey)
    def name = column[String]("name")
    def * = (id, name) <> ((Author.apply _).tupled, Author.unapply)
  }
  val authors = TableQuery[Authors]

  class Books(tag: Tag) extends Table[Book](tag, "book") {
    def id = column[Int]("id", O.PrimaryKey)
    def name = column[String]("name")
    def * = (id, name) <> ((Book.apply _).tupled, Book.unapply)
  }
  val books = TableQuery[Books]

  class AuthorBooks(tag: Tag) extends Table[AuthorBook](tag, "author_book") {
    def authorId = column[Int]("author_id")
    def bookId = column[Int]("book_id")
    def authorFk = foreignKey("author_fk", authorId, authors)(_.id, onUpdate=ForeignKeyAction.Cascade, onDelete=ForeignKeyAction.Cascade)
    def bookFk = foreignKey("book_fk", bookId, books)(_.id, onUpdate=ForeignKeyAction.Cascade, onDelete=ForeignKeyAction.Cascade)
    def pk = primaryKey("pk", (authorId, bookId))
    def * = (authorId, bookId) <> ((AuthorBook.apply _).tupled, AuthorBook.unapply)
  }
  val authorBooks = TableQuery[AuthorBooks]

  // Create a connection to our in-memory database
  val db = Database.forConfig("h2mem1")

  val stephenKing = Author(1, "Stephen King")
  val jkRowling = Author(2, "J. K. Rowling")
  val jrrTolkien = Author(3, "J. R. R. Tolkien")
  val danBrown = Author(4, "Dan Brown")

  val theShining = Book(1, "The Shining")
  val harryPotter = Book(2, "Harry Potter")
  val theLordOfTheRings = Book(3, "Lord of the Rings")
  val theDaVinciCode = Book(4, "The Da Vinci Code")
  val fictitiousBook1 = Book(5, "Ficticious Book 1")
  val fictitiousBook2 = Book(6, "Ficticious Book 2")

  try {
    val setup = DBIO.seq(
      // Create the tables
      (authors.schema ++ books.schema ++ authorBooks.schema).create,

      // Insert some authors
      authors += stephenKing,
      authors += jkRowling,
      authors += jrrTolkien,
      authors += danBrown,

      // Insert some books
      books += theShining,
      books += harryPotter,
      books += theLordOfTheRings,
      books += theDaVinciCode,
      books += fictitiousBook1,
      books += fictitiousBook2,

      // Create normalized many to many relationships
      authorBooks += AuthorBook(stephenKing.id, theShining.id),
      authorBooks += AuthorBook(jkRowling.id, harryPotter.id),
      authorBooks += AuthorBook(jrrTolkien.id, theLordOfTheRings.id),
      authorBooks += AuthorBook(danBrown.id, theDaVinciCode.id),
      authorBooks += AuthorBook(jkRowling.id, fictitiousBook1.id),
      authorBooks += AuthorBook(jrrTolkien.id, fictitiousBook1.id),
      authorBooks += AuthorBook(stephenKing.id, fictitiousBook2.id),
      authorBooks += AuthorBook(danBrown.id, fictitiousBook2.id),
    )

    val setupFuture = db.run(setup)

    val futureResult = setupFuture.flatMap { _ =>
      // Read all authors and print them to the console
      println("Authors:")
      db.run(authors.result).map(_.foreach(println))
    }.flatMap { _ =>
      // Read all books and print them to the console
      println("Books:")
      db.run(books.result).map(_.foreach(println))
    }.flatMap { _ =>
      // Read all normalized relationships
      println("AuthorBooks:")
      db.run(authorBooks.result).map(_.foreach(println))
    }.flatMap { _ =>
      // Read all books by Stephen King
      println("Stephen Kings Books:")
      val booksJoinedToAuthorBooks = books join authorBooks on (_.id === _.bookId)
      val booksFilteredToStephenKing = booksJoinedToAuthorBooks.filter(_._2.authorId === stephenKing.id)
      db.run(booksFilteredToStephenKing.result).map(_.map(_._1).foreach(println))
    }.flatMap { _ =>
      // Read all books by Stephen King
      println("Fictitious Book 2's Authors:")
      val authorsJoinedToAuthorBooks = authors join authorBooks on (_.id === _.authorId)
      val authorsFilteredToFictitiousBook2 = authorsJoinedToAuthorBooks.filter(_._2.bookId === fictitiousBook2.id)
      db.run(authorsFilteredToFictitiousBook2.result).map(_.map(_._1).foreach(println))
    }

    Await.result(futureResult, Duration.Inf)
  } finally db.close

}

