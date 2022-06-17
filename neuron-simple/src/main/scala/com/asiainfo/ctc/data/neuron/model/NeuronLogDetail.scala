package com.asiainfo.ctc.data.neuron.model

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-17
 *
 * @param uuid uuid
 * @param interface_name 接口名
 * @param file_name 文件名
 * @param build_file_status 生成文件状态(1成功/-1:失败/0:未生成)
 */
case class NeuronLogDetail(uuid: String, interface_name: String, file_name: String, build_file_status: String = 1.toString)
