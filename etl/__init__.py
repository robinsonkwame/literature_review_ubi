from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import HTMLConverter,TextConverter,XMLConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from bs4 import BeautifulSoup
import time
import datetime
import io
import requests
from selenium import webdriver
import pandas as pd
from lxml.html.clean import Cleaner
import re
import unicodedata
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ExtractTransformEconSecurityProject(object):
    def __init__(self,
                 data_file_path=None,
                 pdf_url_redirect=None,
                 major_topic_list=None):
        self.data_file_path = data_file_path or\
                "./data/economic_security_project/reading_list.json"
        # some url links are to paywalled academic pdfs, alternatives are given here
        self.pdf_url_redirect = pdf_url_redirect or\
                "./data/economic_security_project/pdf_redirect_url.json"
        # read in redirect url(s), convert to dict for faster access
        self.pdf_redirect = pd.read_json(self.pdf_url_redirect)\
                              .set_index("old_url")\
                              .to_dict()["new_url"]

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

    def _selenium_get(self, url=None, username=None, password=None):
        """
        Using selenium typically requires a login to the company hosting the url,
        e.g., WSJ requires a login to read their content. I don't have a WSJ login
        so this function is left untested and isn't used in the main extract loop.

        See: https://github.com/andrewzc/python-wsj/blob/master/article-parse/v1.2.0/article_parser.py
        """
        ret = None
        browser = webdriver.Firefox()
        browser.get(url)
        login = browser.find_element_by_link_text("Log In").click()
        loginID = browser.find_element_by_id("username").send_keys(username)   # Input username
        loginPass = browser.find_element_by_id("password").send_keys(password) # Input password
        loginReady = browser.find_element_by_class_name("login_submit")
        loginReady.submit()
        html = browser.page_source
        browser.close()
        ret = self._visible_text(BeautifulSoup(html, 'html.parser'))
        return ret

    def _html_to_text(self, url=None, html=None):
        ret = None
        if url and not html:
            try:
                response = requests.get(url, headers=self.headers, timeout=60)
            except requests.exceptions.Timeout:
                pass
            else:
                ret = self._visible_text(BeautifulSoup(response.content, 'html.parser'))
        return ret

    def _visible_text(self, soup):
        """
        Adopted from https://stackoverflow.com/a/44611484/3662899 `Polar Beer` lol
        """
        ret = None
        INVISIBLE_ELEMS = ('style', 'script', 'head', 'title')
        RE_SPACES = re.compile(r'\s{3,}')

        """ get visible text from a document """
        text = ' '.join([
            s for s in soup.strings
            if s.parent.name not in INVISIBLE_ELEMS
        ])
        # collapse multiple spaces to two spaces, throw out unicode &nbsp, etc
        two_spaces = RE_SPACES.sub('  ', text)
        ret = unicodedata.normalize('NFKD', two_spaces)
        return ret

    def _pdf_to_text(self, url=None, pdf=None):
        """
        Convert pdf to text w/o writing to disk

        Adopted from https://stackoverflow.com/a/48825461/3662899 `illusionx`
        Also see: https://stackoverflow.com/a/44458184/3662899 for importance of LAParams
        """
        ret = None
        if url and not pdf:
            if url in self.pdf_redirect:
                url = self.pdf_redirect[url]
            response = requests.get(url, headers=self.headers, verify=False, timeout=360)
            pdf = response.content

        manager = PDFResourceManager()
        output = io.StringIO()
        codec = 'utf-8'
        caching = True
        pagenums = set()
        laparams = LAParams(all_texts=True, detect_vertical=False)
        converter = TextConverter(manager, output, codec=codec, laparams=laparams)
        interpreter = PDFPageInterpreter(manager, converter)

        for page in PDFPage.get_pages(io.BytesIO(pdf),
                                      pagenums,
                                      caching=caching,
                                      check_extractable=True):
            interpreter.process_page(page)

        text = output.getvalue()
        # join together hyphenated words on newlines, replace newlines with space
        newlines = text.replace('-\n', '').replace('\n', ' ')

        # One off fix ups observed in the result, defintely not sustainable
        ret = newlines.replace('T here', 'There')\
                      .replace('consoli date', 'consolidate')

        ret = unicodedata.normalize('NFKD', ret)
        # Seeing cases where unicode non breaking space \xc2\xa0 is normalized as \xc2
        ret = ret.replace(u'\x0c2', '').replace(u'\x0c', '')

        # consider special casing removing footer/header text? Will it affect the Argumentation mining at large scale?
        # I assume not, since we're analyzing larger scale tendencies (e.g., an argument mentioned once, never
        # refuted by someone else probably isn't as important in large scale discourse)

        converter.close()
        output.close()
        return ret

    def download_text(self, content_type="html",
                      exclude_major_topic=[],
                      exclude_hosts=[],
                      selenium_hosts=["wsj.com"],
                      use_selenium=False):
        assert 'url' in self.df, "DataFrame does not have a `url` column to d/l data from! Bad .json read?"
        assert 'Text' in self.df, "DataFrame does not have a `Text` column to push text data to! Run .transform()"
        assert 'Type' in self.df, "DataFrame does not have a `Type` column! Run .transform()"

        extract = self._html_to_text
        if "pdf" == content_type:
            extract = self._pdf_to_text

        total = sum(self.df.Type == content_type)
        count = 0
        logger.info("About to start extract Text from {} urls".format(content_type))
        for idx in self.df.index:
            if self.df.url[idx] and self.df.Type[idx] == content_type\
               and not self.df.MajorTopic[idx] in exclude_major_topic\
               and not any((host in self.df.url[idx] for host in exclude_hosts)):
                url = self.df.url[idx]
                count += 1
                logger.info("({}/{}) Extracting {} ... ".format(count, total, url))
                if any((host in url for host in selenium_hosts)):
                    logger.info("\t ... using Selenium ... you will see a Firefox browser flash on-screen for a few moments ...")
                    text = ""
                    if use_selenium:
                        text = self._selenium_get(url)
                else:
                    text = extract(url)
                self.df.Text[idx] = text
                logger.info("({}/{}) Extracted: \"{}\"".format(count, total, text[:50] if text else "Did Not Fetch Text!"))

                time.sleep(1) # be a good netizen when scraping content

    def transform(self):
        # For content w no url (only occurs once I thnk), we replace 'None'
        # with the empty string so that additional str transformations can proceed easily
        # and the empty string is skipped in a url existence check downstream
        #
        # Doing this replace directly since .replace() gets squirelly if
        # value is actually None, want to replace with None, even though
        # None is the default value
        self.df.url[self.df.url == 'None'] = ''
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

    # note: untested for now
    def load(self, load_filepath="./data/",
             load_filename="download_text_{}.csv"):
        today = datetime.datetime.now()
        self.df.to_csv(os.join(load_filepath,
                               load_filename.format(now.strftime("%Y-%m-%d"))),
                       sep="\t")

