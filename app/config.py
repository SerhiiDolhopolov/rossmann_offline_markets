from dotenv import load_dotenv
import os

load_dotenv()
DATE_TIME_FORMAT = os.getenv("DATE_TIME_FORMAT", "%Y-%m-%d %H:%M:%S") 