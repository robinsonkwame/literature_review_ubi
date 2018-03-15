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
                [{"topic":"Books","upper":7},
                {"topic":"Overview of UBI","upper":20},
                {"topic":"Past Programs, Pilots and Findings","upper":37},
                {"topic":"Current & Pending Pilots and Programs","upper":59},
                {"topic":"Policy Variants and Alternatives","upper":76},
                {"topic":"Political & Policy Change Strategies","upper":91},
                {"topic":"Arguments for UBI","upper":113},
                {"topic":"Critiques and Concerns","upper":128},
                {"topic":"Misc Videos","upper":133}]

    def transform(self):
        self.df[['Source', 'Author', 'Title', 'Misc']] =\
            self.df["raw_content"].str.split("//", expand=True)

        # Set dates
        regex = '\((.+)\)'
        author_dates = self.df['Author']\
                           .str\
                           .extract(regex, expand=False)
        title_dates = self.df['Title']\
                           .str\
                           .extract(regex, expand=False)
        self.df["Date"] = author_dates.combine_first(title_dates)

        # Set major_topics
        self.df["Major Topic"] = None

        lower = 0
        for item in self.topic_list:
            topic = item["topic"]
            upper = item["upper"]
            self.df["Major Topic"].iloc[lower:upper] = topic
            lower = upper
        # swap Books Authors and Title's (I think they're opposite everything else)

        pass
