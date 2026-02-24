# Cleaning Data

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window

from config.detection_conf import SparkConfig
from utils.schema import network_schema
from utils.formatter import clean_data

import os
import json

def ddos_detection_logic(df):
  """
  Hàm này chứa logic phát hiện DDoS dựa trên các quy tắc đã định nghĩa.
  Ví dụ: Nếu một IP gửi hơn 1000 yêu cầu trong vòng 1 phút, có thể coi là DDoS.
  """
  return

