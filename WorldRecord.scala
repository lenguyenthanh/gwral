//> using scala 3.6.nightly
//> using toolkit typelevel:0.1.27
//> using dep io.github.kirill5k::mongo4cats-core:0.7.8
//> using dep io.github.kirill5k::mongo4cats-circe:0.7.8
//> using dep is.cir::ciris:3.6.0
//> using dep com.outr::scribe-cats:3.15.0
//> using dep com.outr::scribe:3.15.0
//> using dep io.circe::circe-core:0.14.9
//> using repository https://raw.githubusercontent.com/lichess-org/lila-maven/master
//> using dep org.lichess::scalachess:16.1.0

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
        .groupWithin(batchSize, 1.second) // config.windows
        .evalTap(_.traverse_(x => IO.println(s"received $x")))
        .map(_.toList.map(_.fullDocument).flatten)
        .evalTap(_.traverse_(x => IO.println(s"full $x")))
        .evalTap(_.traverse_(x => IO.println(s"clock ${x.clock}")))

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
    turn: Int,                      // t
    createdAt: Instant
):
  def clock =
    val f = ClockReader.clock(createdAt).read(encodedClock, whitePlayer.isBerserked, blackPlayer.isBerserked)
    (f(chess.Color.White), (f(chess.Color.Black))).mapN((_, _))

object DbGame:

  given Decoder[DbGame] =
    Decoder.forProduct11("_id", "us", "p0", "p1", "s", "hp", "c", "cw", "cb", "t", "ca")(DbGame.apply)

  given Encoder[DbGame] =
    Encoder.forProduct11("_id", "us", "p0", "p1", "s", "hp", "c", "cw", "cb", "t", "ca")(g =>
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
        g.turn,
        g.createdAt
      )
    )

case class DbPlayer(rating: Option[Int], ratingDiff: Option[Int], berserk: Option[Boolean]):
  def isBerserked = berserk.contains(true)

object DbPlayer:
  given Decoder[DbPlayer] = Decoder.forProduct3("e", "d", "be")(DbPlayer.apply)
  given Encoder[DbPlayer] = Encoder.forProduct3("e", "d", "be")(p => (p.rating, p.ratingDiff, p.berserk))

object ClockReader:
  def x = 1
  import chess.*
  def readClockLimit(i: Int) = Clock.LimitSeconds(if i < 181 then i * 60 else (i - 180) * 15)

  def readConfig(ba: Array[Byte]): Option[Clock.Config] =
    ba match
      case Array(b1, b2, _*) => Clock.Config(readClockLimit(b1), Clock.IncrementSeconds(b2)).some
      case _                 => None

  private inline def toInt(inline b: Byte): Int = b & 0xff
  private val int23Max                          = 1 << 23

  def readInt(b1: Int, b2: Int, b3: Int, b4: Int) =
    (b1 << 24) | (b2 << 16) | (b3 << 8) | b4

  def readInt24(b1: Int, b2: Int, b3: Int) = (b1 << 16) | (b2 << 8) | b3
  def readSignedInt24(b1: Int, b2: Int, b3: Int) =
    val i = readInt24(b1, b2, b3)
    if i > int23Max then int23Max - i else i

  object clock:
    def apply(start: Instant) = new clock(Timestamp(start.toEpochMilli))

  final class clock(start: Timestamp):

    def legacyElapsed(clock: Clock, color: Color) =
      clock.limit - clock.players(color).remaining

    def computeRemaining(config: Clock.Config, legacyElapsed: Centis) =
      config.limit - legacyElapsed

    private def readTimer(l: Int) =
      Option.when(l != 0)(start + Centis(l))

    def read(ba: Array[Byte], whiteBerserk: Boolean, blackBerserk: Boolean): Color => Option[Clock] =
      color =>
        val ia = ba.map(toInt)

        // ba.size might be greater than 12 with 5 bytes timers
        // ba.size might be 8 if there was no timer.
        // #TODO remove 5 byte timer case! But fix the DB first!
        // val timer = if ia.lengthIs == 12 then readTimer(readInt(ia(8), ia(9), ia(10), ia(11))) else None

        ia match
          case Array(b1, b2, b3, b4, b5, b6, b7, b8, _*) =>
            val config      = Clock.Config(readClockLimit(b1), Clock.IncrementSeconds(b2))
            val legacyWhite = Centis(readSignedInt24(b3, b4, b5))
            val legacyBlack = Centis(readSignedInt24(b6, b7, b8))
            val players = ByColor((whiteBerserk, legacyWhite), (blackBerserk, legacyBlack))
              .map: (berserk, legacy) =>
                ClockPlayer
                  .withConfig(config)
                  .copy(berserk = berserk)
                  .setRemaining(computeRemaining(config, legacy))
            Clock(
              config = config,
              color = color,
              players = players,
              timer = None
            ).some
          case _ => None
