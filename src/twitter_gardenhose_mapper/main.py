import argparse
import time
from threading import Thread

import tweepy
import json
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

    def on_exception(self, exception):
        """Called when an unhandled exception occurs."""
        log.warning("on_exception: An exception occurred: {}".format(exception))
        return
    
    def on_timeout(self):
        """Called when stream connection times out"""
        log.warning("on_timeout: was called")
        return
    
    def on_disconnect(self, notice):
        """Called when twitter sends a disconnect notice
        Disconnect codes are listed here:
        https://dev.twitter.com/docs/streaming-apis/messages#Disconnect_messages_disconnect
        """
        log.warning("on_disconnect: A disconnect notice was sent: {}".format(notice))
        log.debug("Disconnect codes are listed here: https://dev.twitter.com/docs/streaming-apis/messages#Disconnect_messages_disconnect")
        return
    
    def on_warning(self, notice):
        """Called when a disconnection warning message arrives"""
        log.warning("on_warning: A warning was sent: {}".format(notice))
        return


# eContext Objects
################################################################################
# Default configuration values.  Really, you should have everything important
# in an actual configuration file.  And *never* store usernames and passwords in
# code that you commit to github.
################################################################################
default_config = {
    'filter_words': 'the, i, to, a, and, is, in, it, you, of, ich, ein, de, la, el, и, être, je',
    'filter_languages': 'en, es, de, fr, pt, ru, tr',
    'filter_locations': '',  # England = -6.3799,49.8712,1.7690,55.8117
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


def map_threads(q, econtext, tpc=500, thread_id=0, sentiment=False, *args, **kwargs):
    working = True
    tweets = []
    total_tweets = 0
    social = Social(econtext, None)
    while working:
        try:
            if len(tweets) == tpc:
                social = Social(econtext, tweets)
                social.data['stream_meta'] = {"namespace": "econtext.social.twitter.gardenhose"}
                social.data['source_language'] = 'auto'
                social.data['sentiment'] = sentiment
                results = social.get_results()
                total_tweets += tpc
                tweets = []
                log.info("{} - Mapping {} tweets in {} seconds - thread total: {}".format(thread_id, tpc, social.get_duration(), total_tweets))
                continue
            
            if q.empty():
                time.sleep(1)
            else:
                w = q.get()
                q.task_done()
                if w is None:
                    working = False
                else:
                    tweets.append(w.text)
        except:
            log.exception("{} - An error occurred during map_threads".format(thread_id))
            log.error("Content for the POST is: {}".format(json.dumps(social.get_data())))
            tweets = []
    
    # signal to the queue that task has been processed
    log.info("Thread {} is shutting down...".format(thread_id))
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
    parser.add_argument("-s", "--sentiment", dest="config_sentiment", default=False, action="store_true", help="Include sentiment in results (increases latency)")
    parser.add_argument("-l", "--languages", dest="config_filter_languages", default=None, help="Language codes to include")

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
        sentiment = config_get(config, 'config', 'sentiment')
        auth = tweepy.OAuthHandler(config_get(config, 'config', 'consumer_key'), config_get(config, 'config', 'consumer_secret'))
        auth.set_access_token(config_get(config, 'config', 'access_token_key'), config_get(config, 'config', 'access_token_secret'))
        stream = tweepy.Stream(auth, listener)

        track = [x.strip() for x in config_get(config, 'config', 'filter_words').split(",")]
        languages = [x.strip() for x in config_get(config, 'config', 'filter_languages').split(",")]
        locations = [float(x.strip()) for x in config_get(config, 'config', 'filter_locations').split(",")]
        
        workers = []
        for i in range(num_threads):
            log.debug('Starting thread {}'.format(i))
            worker = Thread(target=map_threads, args=(q, econtext, tpc, i, sentiment))
            worker.setDaemon(True)
            worker.start()
            workers.append(worker)
        
        if locations:
            stream.filter(locations=locations, languages=languages)
        else:
            stream.filter(track=track, languages=languages)
            
    except Exception:
        log.exception("Caught an Exception in main.py ...")
        while True:
            q.put_nowait(None)
            for i in range(len(workers)):
                workers[i].join(timeout=2)
                if not workers[i].is_alive():
                    log.info("Worker thread {} finished".format(workers[i].ident))
                    workers[i] = None
            workers = [w for w in workers if w is not None]
            if len(workers) == 0:
                q.join()
                break
    
    q.join()
    log.info("Finished...")


if __name__ == '__main__':
    main()
