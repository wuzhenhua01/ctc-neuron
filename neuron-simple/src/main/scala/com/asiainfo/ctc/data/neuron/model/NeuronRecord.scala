package com.asiainfo.ctc.data.neuron.model

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-09
 */
object NeuronRecord {
  val COMMIT_TIME_METADATA_FIELD = "_hoodie_commit_time"
  val COMMIT_SEQNO_METADATA_FIELD = "_hoodie_commit_seqno"
  val RECORD_KEY_METADATA_FIELD = "_hoodie_record_key"
  val PARTITION_PATH_METADATA_FIELD = "_hoodie_partition_path"
  val FILENAME_METADATA_FIELD = "_hoodie_file_name"
  val OPERATION_METADATA_FIELD = "_hoodie_operation"
  val HOODIE_IS_DELETED = "_hoodie_is_deleted"


  val NEURON_META_COLUMNS = List(COMMIT_TIME_METADATA_FIELD, COMMIT_SEQNO_METADATA_FIELD)
}
