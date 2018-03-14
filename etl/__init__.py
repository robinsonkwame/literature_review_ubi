import pandas as pd
import os

class ExtractTransformEconSecurityProject(object):
    def __init__(self,
                 data_file_path=None,
                 major_topic_list=None):
        self.data_file_path = data_file_path or\
                "./data/economic_security_project/reading_list.json"

        self.df = pd.read_json(self.data_file_path)
        self.topic_list = major_topic_list or\
                            [{"Books":7},
                             {"Overview of UBI":20},
                             {"Past Programs, Pilots and Findings":37},
                             {"Current & Pending Pilots and Programs":59},
                             {"Policy Variants and Alternatives":76},
                             {"Political & Policy Change Strategies":91},
                             {"Arguments for UBI":113},
                             {"Critiques and Concerns":128},
                             {"Misc Videos":133}]

    def transform(self):
        self.df[['Source', 'Author', 'Title', 'Misc']] =\
            self.df["raw_content"].str.split("//", expand=True)

        self.df["Date"] = None #... str.rextract on Source, Author
        # set major_topics
        # swap Books Authors and Title's (I think they're opposite everything else)

        pass
