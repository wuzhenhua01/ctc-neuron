package com.asiainfo.ctc.data.neuron.model

trait WriteOperationType {

}

case class INSERT() extends WriteOperationType {

}

case class INSERT_OVERWRITE() extends WriteOperationType {

}
