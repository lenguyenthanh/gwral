//> using scala 3.5.0-RC4
//> using toolkit typelevel:0.1.27
//> using dep ch.qos.logback:logback-classic:1.5.6
//> using dep io.circe::circe-core:0.14.9
//> using dep io.github.kirill5k::mongo4cats-circe:0.7.8
//> using dep io.github.kirill5k::mongo4cats-core:0.7.8
//> using dep is.cir::ciris:3.6.0
//> using repository https://raw.githubusercontent.com/lichess-org/lila-maven/master
//> using dep org.lichess::scalachess:16.1.0

//> using resourceDir .

//> using options -Wunused:all

import cats.effect.*
import cats.syntax.all.*
import ciris.*
import com.mongodb.ReadPreference
import com.mongodb.client.model.changestream.FullDocument
import com.mongodb.client.model.changestream.OperationType.*
import com.monovore.decline.*
import com.monovore.decline.effect.*
import io.circe.*
import java.time.Instant
import mongo4cats.circe.*
import mongo4cats.client.MongoClient
import mongo4cats.collection.MongoCollection
import mongo4cats.database.MongoDatabase
import mongo4cats.operations.{ Aggregate, Filter }
import org.bson.BsonTimestamp
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.syntax.*
import scala.concurrent.duration.*

given Logger[IO] = Slf4jLogger.getLogger[IO]

object cli
    extends CommandIOApp(
      name = "gwral",
      header = "CLI tool for Guinness World Record attempt",
      version = "1.0.0"
    ):

  override def main: Opts[IO[ExitCode]] =
    Args.parse.map(x => execute(x).as(ExitCode.Success))

  def execute(args: Args): IO[Unit] =
    resource(args).useForever

  def resource(args: Args) =
    for
      config <- Config.load.toResource
      _ <- info"Watching games from ${args.since} to ${args.until} in debug mode = ${args.debug}".toResource
      mongoClient <- config.makeClient
      games       <- mongoClient.getCollectionWithCodec[DbGame]("game5").toResource
      watcher     <- GameWatcher(games, args.debug).toResource
      _           <- watcher.watch(args.since, args.until).toResource
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

trait GameWatcher:
  // watch change events from game5 collection
  def watch(since: Instant, until: Instant): IO[Unit]

object GameWatcher:

  def apply(games: MongoCollection[IO, DbGame], debug: Boolean): IO[GameWatcher] =
    Ref.of[IO, Int](0).map(ref => GameWatcher(games, debug, ref))

  def apply(games: MongoCollection[IO, DbGame], debug: Boolean, ref: Ref[IO, Int]): GameWatcher = new:

    def watch(since: Instant, until: Instant): IO[Unit] =
      changes(since, until)
        .evalTap: games =>
          (Writer.writeGames(games), Writer.writeCount(ref, games.size)).parTupled.void
        .compile
        .drain

    private def changes(
        since: Instant,
        until: Instant
    ): fs2.Stream[IO, List[DbGame]] =
      val batchSize  = 1000
      val timeWindow = 10
      games
        .watch(aggreate(since, until))
        .startAtOperationTime(BsonTimestamp(since.getEpochSecond.toInt, 1))
        .batchSize(batchSize)                     // config.batchSize
        .fullDocument(FullDocument.UPDATE_LOOKUP) // this is required for update event
        .boundedStream(batchSize)
        .groupWithin(batchSize, timeWindow.second) // config.windows
        .evalTap(_.traverse_(x => info"received $x").whenA(debug))
        .evalTap(_.filter(_.fullDocument.exists(_.validClock)).traverse_(x => info"count $x"))
        .map(_.toList.map(_.fullDocument).flatten)
        .evalTap(_.traverse_(x => info"clock ${x.clock}").whenA(debug))
        .map(_.filter(_.validClock))
        .evalTap(_.traverse_(x => info"valid clock ${x.clock}").whenA(debug))

    private def aggreate(since: Instant, until: Instant) =
      // games have at least 15 moves
      val turnsFilter    = Filter.gte("fullDocument.t", 30)
      val standardFilter = Filter.eq("fullDocument.v", 1).or(Filter.notExists("fullDocument.v"))
      val ratedFilter    = Filter.eq("fullDocument.ra", true)
      val noAiFilter =
        Filter
          .eq("fullDocument.p0.ai", 0)
          .or(Filter.notExists("fullDocument.p0.ai"))
          .and(Filter.eq("fullDocument.p1.ai", 0).or(Filter.notExists("fullDocument.p1.ai")))

      // Filter games that finished with Mate, Resign, Stalemate, Draw, Outoftime, Timeout
      // https://github.com/lichess-org/scalachess/blob/master/core/src/main/scala/Status.scala#L18-L23
      val statusFilter = Filter.in("fullDocument.s", List(30, 31, 32, 33, 34, 35))

      // filter games that played and ended between since and until
      val playedTimeFilter =
        Filter
          .gte("fullDocument.ca", since)
          .and(Filter.lte("fullDocument.ua", until))

      val updatedStatusOnlyFilter = Filter
        .exists("updateDescription.updatedFields.s")
        .or(Filter.notExists("updateDescription"))

      // required clock config
      val clockFilter        = Filter.exists("fullDocument.c")
      // only human plays
      val noImportGameFilter = Filter.notExists("fullDocument.pgni")

      val gameFilter = standardFilter
        .and(turnsFilter)
        .and(ratedFilter)
        .and(noAiFilter)
        .and(statusFilter)
        .and(updatedStatusOnlyFilter)
        .and(playedTimeFilter)
        .and(clockFilter)
        .and(noImportGameFilter)

      Aggregate.matchBy(gameFilter)

case class DbGame(
    id: String,                     // _id
    players: Option[List[String]],  // us
    whitePlayer: DbPlayer,          // p0
    blackPlayer: DbPlayer,          // p1
    status: Int,                    // s
    huffmanPgn: Array[Byte],        // hp
    encodedClock: Array[Byte],      // c
    encodedWhiteClock: Array[Byte], // cw
    encodedBlackClock: Array[Byte], // cb
    turn: Int,                      // t
    createdAt: Instant,             // ca
    moveAt: Instant                 // ua
):
  def clock               = ClockDecoder.read(encodedClock)
  def validClock: Boolean = clock.exists(_.forall(_.sastify))

val minTotalSeconds = 5 * 60      // 5 minutes
val maxTotalSeconds = 8 * 60 * 60 // 8 hours

object DbGame:

  given Decoder[DbGame] =
    Decoder.forProduct12("_id", "us", "p0", "p1", "s", "hp", "c", "cw", "cb", "t", "ca", "ua")(DbGame.apply)

  given Encoder[DbGame] =
    Encoder.forProduct12("_id", "us", "p0", "p1", "s", "hp", "c", "cw", "cb", "t", "ca", "ua")(g =>
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
        g.createdAt,
        g.moveAt
      )
    )

case class DbPlayer(rating: Option[Int], ratingDiff: Option[Int], berserk: Option[Boolean]):
  def isBerserked = berserk.contains(true)

object Writer:

  import fs2.io.file.{ Files, Flags, Path }

  def writeGames(games: List[DbGame]): IO[Unit] =
    fs2.Stream
      .emits(games)
      .map(x => List(x.id, x.createdAt.getEpochSecond, x.moveAt.getEpochSecond).mkString(","))
      .through(Files[IO].writeUtf8Lines(Path("games.csv"), Flags.Append))
      .compile
      .drain

  def writeCount(ref: Ref[IO, Int], size: Int): IO[Unit] =
    ref
      .updateAndGet(_ + size)
      .flatMap: total =>
        fs2.Stream
          .emit(total.toString)
          .through(Files[IO].writeUtf8(Path("count.txt")))
          .compile
          .drain

extension (config: chess.Clock.Config)

  // over 60 moves
  def estimateTotalSecondsOver60Moves = config.limitSeconds.value + 60 * config.incrementSeconds.value

  // Games are equal to or longer than 3+2 / 5+0 or equivalent over 60 moves (e.g., 4+1, 0+30, etc),
  // but not more than 8h (e.g., no 240+60)
  def sastify: Boolean =
    minTotalSeconds <= config.estimateTotalSecondsOver60Moves &&
      config.estimateTotalSecondsOver60Moves <= maxTotalSeconds

object DbPlayer:
  given Decoder[DbPlayer] = Decoder.forProduct3("e", "d", "be")(DbPlayer.apply)
  given Encoder[DbPlayer] = Encoder.forProduct3("e", "d", "be")(p => (p.rating, p.ratingDiff, p.berserk))

object ClockDecoder:
  import chess.*
  private def readClockLimit(i: Int) = Clock.LimitSeconds(if i < 181 then i * 60 else (i - 180) * 15)

  private inline def toInt(inline b: Byte): Int = b & 0xff

  def read(ba: Array[Byte]): Option[ByColor[Clock.Config]] =
    ByColor: color =>
      ba.take(2).map(toInt) match
        case Array(b1, b2) => Clock.Config(readClockLimit(b1), Clock.IncrementSeconds(b2)).some
        case _             => None

case class Args(since: Instant, until: Instant, debug: Boolean)
object Args:

  import cats.data.Validated

  def parse = (
    Opts.option[Instant](
      long = "since",
      help = "fetch all games since this time",
      short = "s",
      metavar = "time in epoch seconds"
    ),
    Opts
      .option[Instant](
        long = "until",
        help = "optional upper bound time",
        short = "u",
        metavar = "time in epoch seconds"
      ),
    Opts
      .flag(
        long = "debug",
        help = "print debug logs",
        short = "d"
      )
      .orNone
      .map(_.isDefined)
  ).mapN(Args.apply)
    .mapValidated(x =>
      if x.until.isAfter(x.since) then Validated.valid(x)
      else Validated.invalidNel(s"since: ${x.since} must be before until: ${x.until}")
    )

  given Argument[Instant] =
    Argument.from("time in epoch seconds"): str =>
      str.toLongOption.fold(Validated.invalidNel(s"Invalid epoch seconds: $str"))(x =>
        Validated.valid(Instant.ofEpochSecond(x))
      )
