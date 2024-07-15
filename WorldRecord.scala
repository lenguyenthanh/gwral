//> using scala 3.6.nightly
//> using toolkit typelevel:0.1.27
//> using dep io.github.kirill5k::mongo4cats-core:0.7.8
//> using dep io.github.kirill5k::mongo4cats-circe:0.7.8
//> using dep is.cir::ciris:3.6.0
//> using repository https://raw.githubusercontent.com/lichess-org/lila-maven/master
//> using dep org.lichess::scalachess:16.1.0
//> using dep com.outr::scribe-cats:3.15.0
//> using dep com.outr::scribe:3.15.0
//> using dep io.circe::circe-core:0.14.9

// //> using options -Wunused:all

import cats.effect.*
import cats.syntax.all.*
import ciris.*
import com.mongodb.ReadPreference
import com.mongodb.client.model.changestream.OperationType.*
import io.circe.*
import java.time.Instant
import mongo4cats.bson.Document
import mongo4cats.circe.*
import mongo4cats.client.MongoClient
import mongo4cats.collection.MongoCollection
import mongo4cats.database.MongoDatabase
import mongo4cats.models.collection.ChangeStreamDocument
import mongo4cats.operations.{ Aggregate, Filter, Projection }
import scala.concurrent.duration.*
import scribe.cats.io.*

object WorldRecord extends IOApp.Simple:

  def run = app.useForever

  def app =
    for
      config      <- Config.load.toResource
      mongoClient <- config.makeClient
      games       <- mongoClient.getCollectionWithCodec[DbGame]("game5").toResource
      watcher = GameWatcher(games)
      _ <- watcher.watch(Instant.now.minusSeconds(60), Instant.now).compile.drain.toResource
    yield ()

case class MongoConfig(uri: String, name: String):
  def makeClient: Resource[IO, MongoDatabase[IO]] =
    MongoClient
      .fromConnectionString[IO](uri)
      .evalMap(_.getDatabase(name).map(_.withReadPreference(ReadPreference.secondary())))

object Config:
  private def uri  = env("MONGO_URI").or(prop("mongo.uri")).as[String]
  private def name = env("MONGO_DATABASE").or(prop("mongo.database")).as[String]
  def load         = (uri, name).parMapN(MongoConfig.apply).load[IO]

// TODO
// filter
trait GameWatcher:
  // watch change events from game5 collection
  def watch(since: Instant, until: Instant): fs2.Stream[IO, List[DbGame]]

object GameWatcher:

  def apply(games: MongoCollection[IO, DbGame]): GameWatcher = new:

    def watch(since: Instant, until: Instant): fs2.Stream[IO, List[DbGame]] =
      changes(since, until)
        .evalTap: events =>
          IO.println(events)

    private def changes(
        since: Instant,
        until: Instant
    ): fs2.Stream[IO, List[DbGame]] =
      val batchSize = 100
      games.watch
        .batchSize(batchSize) // config.batchSize
        .boundedStream(batchSize)
        .groupWithin(batchSize, 100.second) // config.windows
        .evalTap(_.traverse_(x => debug("received $x")))
        .map(_.toList.map(_.fullDocument).flatten)

case class DbGame(
    id: String,                     // _id
    players: List[String],          // us
    whitePlayer: DbPlayer,          // p0
    blackPlayer: DbPlayer,          // p1
    status: Int,                    // s
    huffmanPgn: Array[Byte],        // hp
    encodedClock: Array[Byte],      // c
    encodedWhiteClock: Array[Byte], // cw
    encodedBlackClock: Array[Byte], // cb
    turn: Int                       // t
)

object DbGame:

  given Decoder[DbGame] =
    Decoder.forProduct10("_id", "us", "p0", "p1", "s", "hp", "c", "cw", "cb", "t")(DbGame.apply)

  given Encoder[DbGame] =
    Encoder.forProduct10("_id", "us", "p0", "p1", "s", "hp", "c", "cw", "cb", "t")(g =>
      (
        g.id,
        g.players,
        g.whitePlayer,
        g.blackPlayer,
        g.status,
        g.huffmanPgn,
        g.encodedClock,
        g.encodedWhiteClock,
        g.encodedBlackClock,
        g.turn
      )
    )

case class DbPlayer(rating: Option[Int], ratingDiff: Option[Int])

object DbPlayer:
  given Decoder[DbPlayer] = Decoder.forProduct2("e", "d")(DbPlayer.apply)
  given Encoder[DbPlayer] = Encoder.forProduct2("e", "d")(p => (p.rating, p.ratingDiff))
