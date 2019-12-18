#!/usr/bin/python
# -*- coding: utf-8 -*-

# 判断字符串是否可以转为数字
def is_num(num):
  try:
    int(num)
    return True
  except ValueError:
    return False
