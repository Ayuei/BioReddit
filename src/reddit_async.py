import asyncio
import re
import json
import pandas as pd
import praw
import tqdm
import os
import sys
import warnings
import pmaw

from typing import List

warnings.filterwarnings("ignore", category=FutureWarning)


class RedditParams:
    def __init__(self, params_fp):
        self.params = json.load(params_fp)


class UrlProcessor:
    def __init__(self):
        self.url_re = re.compile("(?P<url>https?://[^\s]+)")
        self.pubmed_url_re = re.compile("(?P<url>https?://www.ncbi.nlm.nih.gov/p[^\s]+)")

    def get_links(self, text: str):
        all_links = self.url_re.findall(text)
        pubmed_links = self.pubmed_url_re.findall("(?P<url>https?://www.ncbi.nlm.nih.gov/p[^\s]+)")

        return all_links, pubmed_links

    def process_pubmed(self, pubmed_links: List[str]):
        pass


class CommentProcessor:
    def __init__(self):
        pass


def to_csv(data, fp):
    df = pd.DataFrame(data)
    df.to_csv(fp, header=True, index=False, columns=list(df.axes[1]))

    return df


class RedditRetriever:
    def __init__(self, toolname="bot1"):
        praw_api = praw.Reddit(toolname)
        self.pmaw = pmaw.PushshiftAPI(praw=praw_api)

    def get_submissions(self, subreddit, limit=None):
        submissions = self.pmaw.search_submissions(subreddit=subreddit, limit=limit, mem_safe=True)

        return submissions

    def get_comments_from_submissions(self, submissions_ids):
        comments = self.pmaw.search_submission_comment_ids(ids=submissions_ids)
        comment_ids = [comment['id'] for comment in comments]
        comments = self.pmaw.search_comments(ids=comment_ids, limit=None, mem_safe=True)

        return comments


def main():
    rt = RedditRetriever()

    submission = rt.get_submissions('askdocs', limit=None)
    sub_ids = list(to_csv(submission, "asdocs_submissions.csv").loc[:, 'id'])
    comments = rt.get_comments_from_submissions(sub_ids)
    to_csv(comments, "asdocs_comments.csv")


if __name__ == "__main__":
    main()
