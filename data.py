"""main - this is a test script"""

import json
import re
import pickle
import html
import multiprocessing as mp

from textblob import TextBlob

import db

# download from 'https://files.pushshift.io/reddit/comments/'
# decompress file with 'bzip2 -dk filename.bz2'

#  link format
#  https://www.reddit.com//comments/<link id>//<post id>

DEBUG = False
MAX = 500
STATUS = 100
PROCESS_POSTS = True


def main():
    """main"""

    if PROCESS_POSTS:
        print("processing {}".format(MAX))

        # read in n lines
        lines = []
        with open("RC_2007-02", "r") as fin:
            for line in fin:
                if len(lines) < MAX:
                    lines.append(line)
                else:
                    break

        # split work into threads
        pool = mp.Pool(processes=1)  # mp.cpu_count())
        pool.map(do_work, lines)

        # close the pool and wait for the work to finish
        pool.close()
        pool.join()

    # examine results
    database = db.Database()
    print(database.popular_subjects())

    # subj = "post"
    # print("############# {} ############".format(subj))
    # # print([
    # #     (pickle.loads(i), link, batch)
    # #     for i, link, batch
    # #     in database.get_by_subject(subj)])
    # sentences = [pickle.loads(i) for i, _, _ in database.get_by_subject(subj)]
    # sentences.sort(key=lambda x: x.sentiment.polarity)
    # for i in sentences:
    #     print(
    #         i.sentiment.subjectivity*i.sentiment.polarity,
    #         i.sentiment.subjectivity,
    #         i.sentiment.polarity,
    #         str(i))


def do_work(raw_post):
    """work function for multithreaded work"""

    database = db.Database()

    if DEBUG:
        print("processing post")

    post = json.loads(raw_post)

    if post['body'] == "[removed]":
        if DEBUG:
            print("post was removed, skipping")
        return

    # sanitize input a little bit
    body = re.sub(r"[<>_*]", "", html.unescape(post['body']))  # commonly used characters in reddits markdown
    body = re.sub(r"((?:[\.?!]+)?)(?:\r?\n)+", r"\1 ", body)  # stay newlines
    body = re.sub(r"([\.?!]+)\s{2,}", r"\1 ", body)  # sometimes people use lots of spaces instead of periods
    body = re.sub(r"([\.?!]){2,}\s?", r"\1 ", body)  # TextBlob doesn't recognize "..." as sentence a dilimiter

    # sanitize a bit more for text processing
    sanitized_body = re.sub(r"\[(.*?)\]\(.*?\)", r"(\1)", body)  # get rid of links

    # keep original and sanitized text
    post_body = TextBlob(sanitized_body)
    for i in post_body.sentences:
        if len(i.words) < 3:
            continue
        subjects = [
            re.sub("[^a-zA-Z]", "", n.singularize().lower())
            for n, t
            in i.tags
            if 'NN' in t]
        subjects = [j for j in subjects if len(j) > 0]
        if len(subjects) > 0:
            # sanitize subjects
            if DEBUG:
                print("Sentence: {}".format(i))
                print("Subjects: {}".format(", ".join(subjects)))

            database.insert(
                subjects,
                pickle.dumps(i, -1),
                "https://www.reddit.com//comments/{}//{}".format(
                    post["link_id"][3:],
                    post["id"]),
                "RC_2009-01")


if __name__ == '__main__':
    main()
