import argparse
import time
from threading import Thread

import tweepy
from econtext.util.config import load_config, update_config, config_get
from econtext.util.log import log, log_add_stream_handler

from econtextapi.client import Client
from econtextapi.classify import Social

from queue import Queue


class MyStreamListener(tweepy.StreamListener):
    
    def __init__(self, queue, *args, **kwargs):
        super(MyStreamListener, self).__init__(*args, **kwargs)
        self.queue = queue
        
    def on_status(self, status):
        #log.debug(status.text)
        self.queue.put(status)
    
    def on_error(self, status_code):
        log.error("Caught a status_code: {}".format(status_code))


# eContext Objects
################################################################################
# Default configuration values.  Really, you should have everything important
# in an actual configuration file.  And *never* store usernames and passwords in
# code that you commit to github.
################################################################################
default_config = {
    'filter_words': 'the, i, to, a, and, is, in, it, you, of',
    'filter_languages': 'en, es, zh, jp, pt, ru, ar',
    'consumer_key': '',
    'consumer_secret': '',
    'access_token_key': '',
    'access_token_secret': '',
    'econtext_key': '',
    'econtext_secret': '',
    'econtext_base_url': 'https://api.econtext.com'
}


def get_econtext_api(access_key, access_secret, baseurl="https://api.econtext.com/v2", *args, **kwargs):
    log.info("connecting to econtext API")
    return Client(access_key, access_secret, baseurl=baseurl)


def map_threads(q, econtext, tpc=500):
    working = True
    tweets = []
    total_tweets = 0
    while working:
        try:
            if q.empty():
                time.sleep(1)
            else:
                w = q.get()
                if w is None:
                    working = False
                tweets.append(w.text)
                if len(tweets) == tpc:
                    social = Social(econtext, tweets)
                    social.data['stream_meta'] = {"namespace": "econtext.social.twitter.gardenhose"}
                    social.data['source_language'] = 'auto'
                    social.data['sentiment'] = False
                    results = social.get_results()
                    total_tweets += tpc
                    tweets = []
                    log.info("Mapping {} tweets in {} seconds - thread total: {}".format(tpc, social.get_duration(), total_tweets))
        except:
            log.exception("An error occurred during map_threads")
    
    # signal to the queue that task has been processed
    q.task_done()
    return True


def main():
    parser = argparse.ArgumentParser(description='Start the Twitter Gardenhose Mapper')
    parser.add_argument("--config", dest="config_config_file", default="/etc/econtext/twitter.ini", help="Configuration file", metavar="PATH")
    parser.add_argument("-v", dest="config_verbose", action="count", default=0, help="Be more or less verbose")
    parser.add_argument("--consumer-key", dest="config_consumer_key", help="Twitter Consumer Key", metavar="STR")
    parser.add_argument("--consumer-secret", dest="config_consumer_secret", help="Twitter Consumer Secret", metavar="STR")
    parser.add_argument("--access-token-key", dest="config_access_token_key", help="Twitter Access Token Key", metavar="STR")
    parser.add_argument("--access-token-secret", dest="config_access_token_secret", help="Twitter Access Token Secret", metavar="STR")
    parser.add_argument("--econtext-key", dest="config_econtext_key", help="eContext API Key", metavar="STR")
    parser.add_argument("--econtext-secret", dest="config_econtext_secret", help="eContext API Secret", metavar="STR")
    parser.add_argument("--econtext-baseurl", dest="config_econtext_baseurl", help="eContext API base URL", metavar="URL")
    parser.add_argument("-t", "--threads", dest="config_threads", default=10, help="Number of eContext threads to dedicate to mapping", metavar="INT")
    parser.add_argument("--tpc", dest="config_tpc", default=500, help="Number of tweets to include in each eContext call", metavar="INT")

    options = parser.parse_args()
    log_add_stream_handler(options.config_verbose)
    config = load_config(options.config_config_file, default_config)
    for section in {'config'}.difference(set(config.sections())):
        config.add_section(section)
    
    del options.config_config_file
    del options.config_verbose
    
    config_updates = dict()
    for k, v in options.__dict__.items():
        if v is not None:
            section, key = k.split("_", 1)
            if section not in config_updates:
                config_updates[section] = dict()
            config_updates[section][key] = str(v)
    update_config(config, config_updates)
    
    try:
        econtext = get_econtext_api(config_get(config, 'config', 'econtext_key'), config_get(config, 'config', 'econtext_secret'), config_get(config, 'config', 'econtext_baseurl'))
        
        q = Queue(maxsize=0)
        num_threads = int(config_get(config, 'config', 'threads'))
        tpc = int(config_get(config, 'config', 'tpc'))
        listener = MyStreamListener(q)
        auth = tweepy.OAuthHandler(config_get(config, 'config', 'consumer_key'), config_get(config, 'config', 'consumer_secret'))
        auth.set_access_token(config_get(config, 'config', 'access_token_key'), config_get(config, 'config', 'access_token_secret'))
        stream = tweepy.Stream(auth, listener)

        track = [x.strip() for x in config_get(config, 'config', 'filter_words').split(",")]
        languages = [x.strip() for x in config_get(config, 'config', 'filter_languages').split(",")]

        for i in range(num_threads):
            log.debug('Starting thread {}'.format(i))
            worker = Thread(target=map_threads, args=(q, econtext, tpc))
            worker.setDaemon(True)
            worker.start()

        stream.filter(track=track, languages=languages)
            
    except Exception:
        log.exception("Caught an Exception...")
        q.join()
    
    log.info("Finished...")

if __name__ == '__main__':
    main()