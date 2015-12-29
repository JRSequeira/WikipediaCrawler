#!/usr/bin/env python
# -*- coding: utf-8 -*-

from wikitools import wiki
from wikitools import api
from wikitools import category
import cPickle as pickle
import re
import csv
import time
import sys


def update_progress(progress, message):
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


def wikipedia_query(query_params, lang='pt'):
    site = wiki.Wiki(url='http://'+lang+'.wikipedia.org/w/api.php')
    site.login('sequeirawiki', 'wikipedia')
    request = api.APIRequest(site, query_params)
    result = request.query()
    return result[query_params['action']]


def get_category_members(category_name, depth, lang='pt'):
    '''
    Input:
    category_name - The name of a wikipedia category
    depth - The depth limit of iterating through the category tree
    lang - The language of wikipedia used

    Output:
    articles - A list of the titles of all the pages found
    '''
    articles = []
    if depth < 0:
        return articles
    number = 0
    # Begin crawling articles in category
    results = wikipedia_query({'list': 'categorymembers',
                               'cmtitle': category_name,
                               'cmtype': 'page',
                               'cmlimit': '500',
                               'action': 'query'}, lang)

    if 'categorymembers' in results.keys() and len(
            results['categorymembers']) > 0:
        for i, page in enumerate(results['categorymembers']):
            article = page['title']
            number += 1
            articles.append(article)

    # Begin crawling subcategories
    results = wikipedia_query({'list': 'categorymembers',
                               'cmtitle': category_name,
                               'cmtype': 'subcat',
                               'cmlimit': '500',
                               'action': 'query'}, lang)

    subcategories = []
    if 'categorymembers' in results.keys() and len(
            results['categorymembers']) > 0:
        for i, category in enumerate(results['categorymembers']):
            cat_title = category['title']
            subcategories.append(cat_title)
        print 'Finished crawling %s, found %d pages and %d sub-categories with depth %d' % (category_name, number, len(subcategories), (7 - depth))
        for category in subcategories:
            articles += get_category_members(category, depth-1)

    return articles


def getenglishname(title):
    result = wikipedia_query({'titles': title,
                             'action': 'query',
                              'prop': 'langlinks'})

    if result and 'pages' in result.keys():
        page_number = result['pages'].keys()[0]
        if 'langlinks' in result['pages'][page_number].keys():
            revisions = result['pages'][page_number]['langlinks']
            english = [lang['*'] for lang in revisions if 'en' in lang['lang']]
            if len(english) > 0:
                return english[0].encode('utf8')
        return ""


def main():
    categories = ['Medicina', 'Saúde', 'Farmacologia', 'Bioquímica']
    for cat in categories:
        print 'Crawl of category %s started' % cat
        categorymembers = get_category_members('Categoria:' + cat, 7, 'pt')
        categorymembers = list(set(categorymembers))
        with open((cat + 'list'), 'wb') as f:
            pickle.dump(categorymembers, f)
            f.close()
        pt_en_dic = create_pt_en_dic(categorymembers)
        meshdic, namedic = mesh_or_name(pt_en_dic)
        savedic(meshdic, 'mesh'+cat)
        savedic(namedic, 'name'+cat)
        print 'Crawl of category %s ended' % cat


def savedic(dict, name):
    w = csv.writer(open(name + ".csv", "w+"))
    for key, val in dict.items():
        w.writerow([key.encode('utf8'), val])


def mesh_or_name(dic):
    meshdic = dict()
    namedic = dict()
    p = re.compile(r"MeshID\s*=\s*(?P<id>[\w\d]+)")
    for k in dic.keys():
        meshid = findmesh(dic[k], p)
        if meshid == "":
            namedic[k] = dic[k]
        else:
            meshdic[k] = meshid
    return meshdic, namedic


def findmesh(string, p):
    s = p.search(get_page_content(string, 'en'))
    if s:
        return s.group(1).encode('utf8')
    else:
        return ""


def get_page_content(page_title, lang):
    '''
    Input:
    page_title - The page to crawl
    lang - The language of wikipedia used

    Output:
    rev  - The content of the latest revision of the page
    '''
    article_title = rename_on_redirect(page_title)
    result = wikipedia_query({'titles': page_title,
                              'prop': 'revisions',
                              'rvprop': 'content',
                              'action': 'query'}, lang)
    rev = ""
    if result and 'pages' in result.keys():
        page_number = result['pages'].keys()[0]
        if 'revisions' in result['pages'][page_number].keys():
            revisions = result['pages'][page_number]['revisions']
            rev = revisions[0]['*']
    return rev


def rename_on_redirect(article_title, lang='en'):
    '''
    Input:
    article_title - the name of the page that may be redirected
    lang - the language of wikipedia used

    Output:
    article_title - the title of the page that is the result of the redirection
                    (if needed)
    '''
    result = wikipedia_query({'titles': article_title,
                              'prop': 'info',
                              'action': 'query',
                              'redirects': 'True'}, lang)
    if 'redirects' in result.keys() and 'pages' in result.keys():
        article_title = result['redirects'][0]['to']
    return article_title


def create_pt_en_dic(categorymembers):
    pt_en_dic = dict()
    total = float(len(categorymembers))
    for j, i in enumerate(categorymembers):
        m = '\r%d out of %d' % (j, total)
        update_progress(j/total, m)
        en = getenglishname(i)
        if en is not "":
            pt_en_dic[i] = en
        else:
            print 'No english link for ', i
    return pt_en_dic


if __name__ == '__main__':
    main()
