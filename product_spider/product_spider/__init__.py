
from datetime import datetime

def get_timestamp_score():
    """timestamp score for redis zincrby, let earlier requests come first"""
    return 1.0 / datetime.now().timestamp()