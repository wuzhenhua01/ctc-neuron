package com.asiainfo.ctc.data.neuron.model

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-17
 *
 * @param uuid UUID
 * @param interface_name 接口名
 * @param data_date 账期
 * @param serial_num 频度(1:日/2:月)
 * @param repeat_no 重传号
 * @param begin_time 程序开始时间
 * @param end_time 程序结束时间
 * @param take_time 程序耗时
 * @param local_path 保存本地目录
 * @param upload_status 上传状态(1:成功/0:失败)
 * @param upload_begin_time 上传开始时间
 * @param upload_end_time 上传结束时间
 * @param data_file_num 文件大小
 * @param check_file_num check文件大小
 * @param upload_path 上传路径
 * @param status 状态(1:成功/0:失败)
 * @param data_file_name 文件名称
 * @param check_file_name CHECK文件名
 * @param data_row_count 总条数
 * @param receipt_status 回执状态(1:成功/0:未回执/-1:失败)
 * @param receipt_msg 回执信息
 * @param build_file_status 生成文件状态(1:成功，0:失败)
 * @param send_smg_status 插入短信表状态(0:未插入/1:已插入/-1:失败)
 * @param receipt_host 回执主机
 * @param receipt_time 回执时间
 */
case class NeuronLog(
    uuid: String = UUID.randomUUID.toString.replaceAll("-", ""),
    interface_name: String,
    data_date: String,
    serial_num: String,
    repeat_no: String,
    begin_time: String = DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(LocalDateTime.now()).replace('T', ' '),
    var end_time: String = null,
    take_time: Long = 0,
    local_path: String,
    upload_status: String = 1.toString,
    upload_begin_time: String = null,
    upload_end_time: String = null,
    var data_file_num: String = 0.toString,
    var check_file_num: String = 0.toString,
    upload_path: String = null,
    status: String = 1.toString,
    data_file_name: String = null,
    var check_file_name: String = null,
    var data_row_count: Long = 0,
    receipt_status: String = 0.toString,
    receipt_msg: String = null,
    build_file_status: String = 1.toString,
    send_smg_status: String = 0.toString,
    receipt_host: String = null,
    receipt_time: String = null)
