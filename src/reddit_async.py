import re
import json
import pandas as pd
import praw
import tqdm
import os
import sys
import warnings
import pmaw
import argparse
from loguru import logger


from typing import List

warnings.filterwarnings("ignore", category=FutureWarning)
logger.add("scrapper.log", rotation="500 MB")

parser = argparse.ArgumentParser(description="A reddit subreddit scrapper")
parser.add_argument("subreddits", type=str, nargs="+", help='Subreddits to scrape')
parser.add_argument("--load_submissions", action='store_true', help='Whether to load submissions from CSV if it exists')
parser.add_argument("--skip", action='store_true', help='Skip subreddits that are ' +
                                                        'already scrapped (may not be up to date)')
parser.add_argument("--test", action='store_true', help='Sets the limit to 10 for testing')
args = parser.parse_args()



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
    logger.info(f"Writing data to {fp}")
    df = pd.DataFrame(data)
    df.to_csv(fp, header=True, index=False, columns=list(df.axes[1]))

    return df


class RedditRetriever:
    def __init__(self, toolname="bot1"):
        praw_api = praw.Reddit(toolname)
        self.pmaw = pmaw.PushshiftAPI(praw=praw_api)

    def get_submissions(self, subreddit, limit=None):
        submissions = self.pmaw.search_submissions(subreddit=subreddit, limit=limit,
                                                   mem_safe=True)

        return submissions

    def get_comments_from_submissions(self, submissions_ids):
        comments = self.pmaw.search_submission_comment_ids(ids=submissions_ids)
        comment_ids = [comment['id'] for comment in comments]
        comments = self.pmaw.search_comments(ids=comment_ids, limit=None, mem_safe=True)

        return comments


@logger.catch
def main():
    rt = RedditRetriever()
    limit = None

    if args.test:
        limit = 10

    for subreddit in args.subreddits:
        logger.info(f"Scrapping {subreddit}")
        if args.load_submissions and os.path.isfile(f'{subreddit}_submissions.csv'):
            logger.info(f"Loading {subreddit} submissions from CSV")
            sub_ids = list(pd.read_csv(f'{subreddit}_submissions.csv', header=0).loc[:, 'id'])
            if limit:
                sub_ids = sub_ids[:limit]
        else:
            logger.info(f"Getting {subreddit} submissions from API")
            submission = rt.get_submissions(subreddit, limit=limit)
            sub_ids = list(to_csv(submission, f'{subreddit}_submissions.csv').loc[:, 'id'])

        if args.skip and os.path.exists(f"{subreddit}_comments.csv"):
            logger.info(f"Skipping {subreddit} comment retrieval...")
            continue

        logger.info(f"Getting {subreddit} comments")
        comments = rt.get_comments_from_submissions(sub_ids)
        to_csv(comments, f"{subreddit}_comments.csv")


if __name__ == "__main__":
    main()
