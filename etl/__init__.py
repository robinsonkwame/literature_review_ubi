from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import HTMLConverter,TextConverter,XMLConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
import time
import io
import requests
import pandas as pd
from lxml.html.clean import Cleaner
import re
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

        self.cleaner = Cleaner()
        self.cleaner.javascript = False
        self.cleaner.style = False

        self.headers = {'User-Agent': 'UBI Fetcher Version 0.1'}

    def _swap(self, to_col, from_col, fix_up):
        self.df[to_col].iloc[fix_up] = self.df[from_col].iloc[fix_up]
        self.df[from_col].iloc[fix_up] = None

    def _html_to_text(self, url=None, html=None):
        if url and not html:
            text = requests.get(url, headers=self.headers).text

        ret = " ".join(\
                re.sub(r'<[^>]*?>', '',\
                    self.cleaner.clean_html(text))\
                .strip()\
                .replace("\n","")
                .split()
                )

        # Cleaner seems to fail on some style related tags
        # note: this does not catch them all, todo: improve
        ret = re.sub(r'{\s*.+?}', '', ret)
        return ret

    def _pdf_to_text(self, url=None, pdf=None):
        """
        Convert pdf to text w/o writing to disk

        Adopted from https://stackoverflow.com/a/48825461/3662899 `illusionx`
        """
        ret = None
        if url and not pdf:
            response = requests.get(url, headers=self.headers, verify=False)
            pdf = response.content

        manager = PDFResourceManager()
        output = io.StringIO()
        codec = 'utf-8'
        caching = True
        pagenums = set()

        converter = TextConverter(manager, output, codec=codec, laparams=LAParams())
        interpreter = PDFPageInterpreter(manager, converter)

        for page in PDFPage.get_pages(io.BytesIO(pdf),
                                      pagenums,
                                      caching=caching,
                                      check_extractable=True):
            interpreter.process_page(page)

        converter.close()
        output.close()

        ret = output.getvalue()
        return ret

    def download_text(self, type="html",
                      exclude_major_topic=[],
                      exclude_host=["amazon"]):
        assert 'url' in self.df, "DataFrame does not have a `url` column to d/l data from! Bad .json read?"
        assert 'Text' in self.df, "DataFrame does not have a `Text` column to push text data to! Run .transform()"
        assert 'Type' in self.df, "DataFrame does not have a `Text` column to push text data to! Run .transform()"

        extract = self._html_to_text
        if "pdf" == type:
            extract = self._pdf_to_text

        total = sum(self.df.Type == type)
        count = 0
        logger.info("About to start extract Text from {} urls".format(type))
        for idx in self.df.index:
            if self.df.Type[idx] == type and\
               not self.df.MajorTopic[idx] in exclude_major_topic and\
               not any((host in self.df.url[idx] for host in exclude_host)):
                url = self.df.url[idx]
                count += 1
                logger.info("({}/{}) Extracting {} ... ".format(count, total, url))
                text = extract(url)
                self.df.Text[idx] = text
                logger.info("({}/{}) Extracted: \"{}\"".format(count, total, text[:50]))

                time.sleep(1) # be a good netizen when scraping content

            if count > 10:
                break
                logging.info("... debug jumping out ...")

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
        self.df["MajorTopic"] = None

        lower = 0
        for item in self.topic_list:
            topic = item["topic"]
            upper = item["upper"]
            self.df["MajorTopic"].iloc[lower:upper] = topic
            lower = upper

        # remove dates from non date columns
        self.df['Author'] =\
                self.df['Author'].str.replace(regex, "")
        self.df['Title'] =\
                self.df['Title'].str.replace(regex, "")

        # One off fix ups/swaps of author and titles
        fix_up = [7,8,15,20,
                  23,25,27,
                  57,60,90,
                  129,130,66]
        self._swap("Title", "Author", fix_up)

        # One off fix ups/swaps of source and titles
        fix_up = [0,1,2,
                  3,4,5,
                  6,31,81,
                  95]
        self._swap("Title", "Source", fix_up)

       # ... a straggler
        self.df["Date"].iloc[32] = "Jan, 2005"
        self.df["Title"].iloc[32] = "A Failure to Communicate: What (If Anything) Can we Learn from the Negative Income Tax Experiments?"

        # label content type as one of html, pdf, audio, video
        self.df["Type"] = "html"
        self.df.Type[self.df["url"].str.contains("pdf") ] = "pdf"
        self.df.Type[self.df["url"].str.contains("podcast") ] = "audio"
        self.df.Type[self.df["url"].str.contains("youtube") ] = "video"
        self.df.Type[self.df["url"].str.contains("ted.com") ] = "video"

        # Place holder for download text to store data into
        self.df["Text"] = None
