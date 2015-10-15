package com.donson.common

import java.util.regex.Pattern

/**
 * Created by Jiawei on 10/15/15.
 */
object Utils {

  /**
   * 判断是否全部是汉字
   * @param str
   * @return boolean
   */
  def isAllCH(str: String) = Pattern.compile("[\\u4e00-\\u9fa5]+").matcher(str).matches()

  /**
   * 判断是否含有汉字
   * @param str
   * @return boolean
   */
  def isContainCH(str: String) = Pattern.compile("[\\u4e00-\\u9fa5]+").matcher(str).find()

  /**
   * 判断是否全部是数字
   * @param str
   * @return boolean
   */
  def isAllNumber(str: String) = Pattern.compile("[0-9]*").matcher(str).matches()

  /**
   * 判断是否含有数字
   * @param str
   * @return boolean
   */
  def isContainNumber(str: String) = Pattern.compile("[0-9]*").matcher(str).find()

}
