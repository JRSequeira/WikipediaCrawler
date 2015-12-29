#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pymongo
from collections import defaultdict
from wikitools import wiki
from wikitools import api
import sys
import time
import urllib2


logger = open('logger', 'wb')

def update_progress(progress, message=""):
    barLength = 10 # Modify this to change the length of the progress bar
    status = ""
    if isinstance(progress, int):
        progress = float(progress)
    if not isinstance(progress, float):
        progress = 0
        status = "error: progress var must be float\r\n"
    if progress < 0:
        progress = 0
        status = "Halt...\r\n"
    if progress >= 1:
        progress = 1
        status = "Done...\r\n"
    block = int(round(barLength*progress))
    text = '\r'+message + "\tPercent: [{0}] {1}% {2}".format( "#"*block + "-"*(barLength-block), progress*100, status)
    sys.stdout.write(text)
    sys.stdout.flush()


def main():
    entries = get_all_info_from_DB()
    dic, counter = get_info_links(entries)
    pt_en = create_pt_en_dic(dic, counter)
    savedic(pt_en)
    logger.close()


def savedic(dict):
    '''
    Saves a dictionary to a mongoDB file

    Input:
    dict - The dictionary to write
    '''

    c = pymongo.Connection()
    db = c.wikipedia
    print "Writing to DBn"
    for key, val in dict.items():
        db.npages.update({"en_name" : key}, {'$set': {'pt_name' : val}}, True)


def create_pt_en_dic(d, c):
    nDic = dict()
    c = float(c)
    j = 0
    for k in d.keys():
        for i in d[k]:
            j += 1
            update_progress(j/c, "Getting pt_name %d of %d" % (j, c))
            pt = get_pt_name(i)
            if pt != "":
                nDic[i] = pt

    return nDic


def wikipedia_query(query_params, lang='pt'):
    try:
        site = wiki.Wiki(url='http://'+lang+'.wikipedia.org/w/api.php')
        site.login('sequeirawiki', 'wikipedia')
        request = api.APIRequest(site, query_params)
        result = request.query()
        if query_params['action'] in result.keys():
            return result[query_params['action']]
        else:
            return None
    except api.APIError:
        sys.stdout.write("DB Error getting " + query_params["titles"].encode('utf8') + ". Sleeping for 30 seconds\n")
        sys.stdout.flush()
        return ""
    except urllib2.URLError:
        logger.write("Error getting " + query_params["titles"].encode('utf8') + "\n")
        return "@@Error@@"
    except AttributeError:
        logger.write("Error getting " + query_params["titles"].encode('utf8') + " got APIListResult")
        return "@@Error@@"

def get_pt_name(title):
    sys.stdout.write("Title = %s\n" % title.encode('utf8'))
    sys.stdout.flush()
    result = ""
    counter = 1
    while result == "":
        result = wikipedia_query({'titles': title,
                                     'action': 'query',
                                      'prop': 'langlinks'}, 'en')
        if result == "":
            sys.stdout.write("Stopped %d times\n" % counter)
            sys.stdout.flush()
            time.sleep(2*counter)
        elif result == "@@Error@@":
            break

    pt_name = ""

    if result and result != "@@Error@@":
        if result and 'pages' in result.keys():
            page_number = result['pages'].keys()[0]
            if 'langlinks' in result['pages'][page_number].keys():
                revisions = result['pages'][page_number]['langlinks']
                english = [lang['*'] for lang in revisions if 'pt' in lang['lang']]
                if len(english) > 0:
                    pt_name = english[0].encode('utf8')
    sys.stdout.write("pt_name = %s\n" % pt_name)
    return pt_name


def get_info_links(entries):
    d = defaultdict(list)
    c = 0
    for e in entries:
        if e["pt_name"] == "":
            t = e["en_name"]
            c +=1
            d[t[0]] += [t]
    return d,c 

def get_all_links(entries):
    d = defaultdict(list)
    c = 0
    for e in entries:
        for l in e["links"]:
            if '|' in l:
                for s in l.split('|'):
                    if 0 < len(s) <= 30:
                        if s not in d[s[0]]:
                            c += 1
                            d[s[0]] += [s]
            else:
                if len(l) < 30:
                    if l not in d[l[0]]:
                        c += 1
                        d[l[0]] += [l]
    return d, c


def get_all_info_from_DB():
    c = pymongo.Connection()
    db = c.wikipedia
    entries = list(db.npages.find({"infobox": {"$exists": True}}))
    return entries


if __name__ == '__main__':
    main()
