package music.recommendation.utility

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession

/**
  * Created by deveshkandpal on 4/18/17.
  */
object FileLocations {

  val RAW_SONG_INFO_LOCATION = "/Users/deveshkandpal/Code/Spark/dataset/" +
    "subset/msd/MillionSongSubset/AdditionalFiles/subset_unique_tracks.txt"


  val RAW_USER_TASTE_LOCATION = "/Users/deveshkandpal/Code/Spark/dataset/subset/" +
    "tasteprofile/tasteprofile.txt"

  val RAW_WORD_DICTIONARY_LOCATION = "/Users/deveshkandpal/Code/Spark/dataset/subset/musicxmatch/wordDictionary.txt"

  val RAW_TRACK_LYRICS_LOCATION = "/Users/deveshkandpal/Code/Spark/dataset/subset/musicxmatch/lyrics.txt"


  // Model locations
  val ALS_MODEL_TRAIN_LOCATION = "/Users/deveshkandpal/Code/Spark/dataset/finalmodels/als/"

  val ARTIST_K_MEANS_MODEL_TRAIN_LOCATION = "/Users/deveshkandpal/Code/Spark/dataset/finalmodels/kmeans/artist"
  val LYRICS_USERS_K_MEANS_MODEL_TRAIN_LOCATION = "/Users/deveshkandpal/Code/Spark/dataset/finalmodels/kmeans/userlyrics"
  val LYRICS_SONGS_K_MEANS_MODEL_TRAIN_LOCATION = "/Users/deveshkandpal/Code/Spark/dataset/finalmodels/kmeans/songlyrics"



}
