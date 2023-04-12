import os
import glob

class CleanFolder():

    def clean_folder(self):
        """Clean temp folder"""
        files = glob.glob("/opt/airflow/code/tmp/*.csv")
        for f in files:
            os.remove(f)