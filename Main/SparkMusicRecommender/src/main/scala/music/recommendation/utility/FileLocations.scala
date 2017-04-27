package music.recommendation.utility

/**
  * Created by deveshkandpal on 4/18/17.
  */
object FileLocations {

  val RAW_SONG_INFO_LOCATION = "C:\\MusicRecommenderSystem\\MillionSongSubset\\AdditionalFiles\\subset_unique_tracks.txt"


  val RAW_USER_TASTE_LOCATION = "C:\\MusicRecommenderSystem\\tasteprofile\\tasteprofile.txt"

  val RAW_WORD_DICTIONARY_LOCATION = "C:\\ScalaProject\\wordDictionary.txt"

  val RAW_TRACK_LYRICS_LOCATION = "C:\\ScalaProject\\lyrics.txt"


  // Model locations
  val ALS_MODEL_TRAIN_LOCATION = "C:\\ScalaProject\\realtimemusicrecommendationapp-master\\Main\\finalmodels\\als\\"

  val ARTIST_K_MEANS_MODEL_TRAIN_LOCATION = "C:\\ScalaProject\\realtimemusicrecommendationapp-master\\Main\\finalmodels\\kmeans\\artist"
  val LYRICS_USERS_K_MEANS_MODEL_TRAIN_LOCATION = "C:\\ScalaProject\\realtimemusicrecommendationapp-master\\Main\\finalmodels\\kmeans\\userlyrics"
  val LYRICS_SONGS_K_MEANS_MODEL_TRAIN_LOCATION = "C:\\ScalaProject\\realtimemusicrecommendationapp-master\\Main\\finalmodels\\kmeans\\songlyrics"



}
