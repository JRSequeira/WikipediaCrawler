#!/usr/bin/env python
# -*- coding: utf-8 -*-

import MySQLdb
import csv
import re
import sys
import pymongo
import pickle
import unicodedata
from collections import defaultdict 
from itertools import tee, izip, groupby
import ast
import re


id_convert = {u'ICD9': 'ICD9CM', u'OMIM': 'OMIM', u'MedlinePlus': 'MEDLINEPLUS', u'FMA': 'FMA', u'HGNCid': 'HGNC'}


def update_progress(progress, message=""):
    barLength = 10  # Modify this to change the length of the progress bar
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
    text = '\r'+message + "\t\tPercent: [{0}] {1}% {2}".format( "#"*block + "-"*(barLength-block), progress*100, status)
    sys.stdout.write(text)
    sys.stdout.flush()


def main():
    print "Connecting to MongoDB"
    f, nf = get_links_from_DB()
    print "Got Links"
    """with open("found", 'wb') as fo:
                    for i, j in f.items():
                        fo.write("%s : " % i.encode('utf-8'))
                        fo.write(repr(j))
                        fo.write('\n')
                with open("nfound", 'wb') as fo:
                    for i, j in nf.items():
                        fo.write("%s : " % i.encode('utf-8'))
                        fo.write(j.encode('utf-8'))
                        fo.write('\n')
    """

    #db = MySQLdb.connect(host="biodatacenter.ieeta.pt",
    #                                          user="umlsmeta",
    #                                          passwd="umlsmeta",
    #                                          db="umlsmeta_2012AB",
    #                                          port=3307)
    #cur = db.cursor()
    #id_d = connect_id(f, cur)
    fname, dname = connect_name(nf)
    #print id_d
    #save_id_dic(id_d, 'dicID')
    write(dname, 'disambig')
    savedic(fname, 'dicname')

def save_id_dic(dic, name):
    w = csv.writer(open(name + ".csv", "w+"))
    for k in dic:
        for item in dic[k]:
            w.writerow([k.encode('utf-8'), item[0], item[1]])



def write(d, name):
    with open(name + '.txt', 'wb') as f:
        for l in d.keys():
            f.write(l.encode('utf-8') + " ---> [")
            for i, item in enumerate(d[l]):
                f.write(item)
                if i == len(d[l])-1:
                    f.write("]")
                else:
                    f.write(', ')

            f.write('\n')


def savedic(dic, name):
    w = csv.writer(open(name + ".csv", "w+"))
    for k in dic:
        print type(k)
        w.writerow([k.encode('utf-8'), dic[k][0], dic[k][1], dic[k][2]])


def connect_name(l):
    print "Depickling dictionary"
    with open('unicodeDic', 'rb') as f:
        d = pickle.load(f)
        f.close()
    print "Done"
    found = defaultdict(list)
    dfound = defaultdict(list)
    total = len(l)
    fcounty = 0
    fcountn = 0
    dcounty = 0
    dcountn = 0
    for e in l.keys():
        uni = l[e].decode('latin-1')
        k = norm_uni(uni[0])
        k2 = norm_uni(uni)
        if k in d.keys():
            if k2 in d[k]:
                mylist = d[k][k2]
                clean = list(set(zip(mylist[0::3], mylist[1::3], mylist[2::3])))
                pref = [x for x in clean if x[1] == 'Y']
                cleanpref = []
                for x in pref:
                    if len(cleanpref) == 0:
                        cleanpref += [x]
                    elif x[0] not in [y[0] for y in cleanpref]:
                        cleanpref += [x]
                if len(cleanpref) > 0:
                    if len(cleanpref) == 1:
                        found[e] += [cleanpref[0][0], cleanpref[0][1], cleanpref[0][2]]
                        #print "FOUND CUI FOR %s -> %s -> Y" % (e, pref[0])
                        fcounty+=1
                    else:
                        dfound[e] += [x[0] for x in cleanpref]
                        #print "NEED TO DISAMBIG %s -> Y -> %s" % (e, ','.join([x[0] for x in pref]))
                        dcounty+=1
                else:
                    print "ENTROU AQUI"   
                    if len(clean) == 1:
                        found[e] += [clean[0][0], clean[0][1], clean[0][2]]
                        #print "FOUND CUI FOR %s -> %s -> N" % (e, clean[0])
                        fcountn+=1
                    else:
                        dfound[e] += [x[0] for x in clean]
                        #print "NEED TO DISAMBIG %s -> N -> %s" % (e, ','.join([x[0] for x in pref]))
                        dcountn+=1

    print "Found Y: " + str(fcounty)
    print "Disambig Y: " + str(dcounty)
    print "Found N: " + str(fcountn)
    print "Disambig Y: " + str(dcountn)
    foundN = fcounty + fcountn
    dis = dcounty + dcountn
    print "Found : %d" % foundN
    print "Disambig : %d" %  dis
    print "Lost : %d"  % (total - (foundN + dis))
    return found, dfound


def norm_uni(uni):
    norm = replace(''.join((c for c in unicodedata.normalize('NFD', uni) if unicodedata.category(c) != 'Mn')))
    return norm


def replace(str):
    return re.sub(r'(\s+\-\s+)|\s+', '-', str).lower()


def pairwise(iterable):
    a = iter(iterable)
    return izip(a, a)


def getname(dic, db):
    foundname = []
    nfoundname = []
    size = len(db)
    total = float(len(dic))
    for i, key in enumerate(dic):
        exact = []
        relative = []
        for j, entry in enumerate(db):
            update_progress((i+1)/total, "Key: %d of %d --- Entry %d of %d " % (i+1, total, j+1, size))
            if key == entry[1].decode('latin1'):
                exact += [[key, entry[0], 'True', entry[2]]]
            elif checkequal(key, entry[1].decode('latin1')):
                relative += [[key, entry[0], 'Relative', entry[2]]]
        if len(exact) > 1:
            f = [x for x in exact if x[3] == 'Y']
            if len(f) > 1:
                nfoundname += f
            else:
                foundname += f
        else:
            foundname += exact
        if len(relative) > 1:
            f = [x for x in relative if x[3] == 'Y']
            if len(f) > 1:
                nfoundname += f
            else:
                foundname += f
        else:
            foundname += relative
        
    return foundname, nfoundname


def connect_id(found, cur):
    total = float(len(found.keys()))
    c = 0
    d = defaultdict(list)
    for i, k in enumerate(found.keys()):
        update_progress((i+1)/total, "%d out of %d " % (i+1, int(total)))
        tags = [x for x in found[k].keys() if x != u'ICD10']
        if len(tags) > 0:
            for t in tags:
                to_query = get_tags(found[k][t], t)
                if to_query != []:
                    result = query_umls(to_query, t, cur)
                    result = clean_query(result, d[k])
                    if result != []:
                        d[k] = result
    return d


def clean_query(query, l):
    aux = []

    for k, group in groupby(query, lambda x: x[0]):
        aux = []
        if len(l) > 0:
            pref = [x for x in l if x[1] == 'Y']
            npref = [x for x in l if x[1] == 'N']
        else:
            pref = []
            npref = []
        if pref == []:
            if 'Y' in [x[3] for x in list(group)]:
                aux += [(k, 'Y')]
            else:
                aux += [(k, 'N')]
        else:
            m = list(group)
            tags = [x[0] for x in pref]
            if 'Y' in [x[3] for x in m if k not in tags]:
                aux += [(k, 'Y')]
            else:
                tags = [x[0] for x in npref]
                if 'Y' in [x[3] for x in m if k in tags]:
                    aux += [(k, 'Y')]
                else:
                    aux += [(k, 'N')]

        l = aux + pref
        for n in npref:
            if x[0] not in [x[0] for x in l]:
                l += [n]

    return l



def clean_string(text, remove):
    for sub_str in [x for x in sorted(remove, key=len, reverse = True)]:
        text = text.replace(sub_str.rstrip() , "") 
        text = text.replace("}}", "") 
    return text

def resolve_clean_ICD9(patternlist, text):
    
    if len(patternlist) == 0:
        return []
    found = re.findall(patternlist[0], text)
    if len(found) > 0:
        text = clean_string(text, found)

    if text != "" and re.search('\d', text):
        patternlist = patternlist[1:]
        found += resolve_clean_ICD9(patternlist, text)
    found = [x.replace("{{ICD9proc|", "").rstrip() for x in found]
    found = [x.replace("{{ICD9|", "").rstrip() for x in found]
    return found


def resolve_clean_Medline(text):
    return ['T' + re.sub(r"^0+", "", text)]


def get_tags(text, t):
    l = []
    print text
    if t == "ICD9":
        l = resolve_clean_ICD9([r"{{ICD9proc\|\w*\d+\.\d+\s*", r"{{ICD9proc\|\w*\d+\s*", r"{{ICD9\|\w*\d+\.\d+\s*", r"{{ICD9\|\w*\d+\s*", r"\b\d+\.\d+"], text)
    elif t == "OMIM":
        l += [text]
    elif t == "MedlinePlus":
        l = resolve_clean_Medline(text)
    elif t == "FMA":
        if re.match(r'\d+', text):
            l += [text]
    elif t == "HGNCid":
        l += [text]
    elif t == "MeshID":
        l += [text]

    return l

def query_umls(id_list, tag, cur):
    #print t + " -> " + tags
    res = []
    if tag != "MeshID":
        query = "SELECT CUI, STR, LAT, ISPREF FROM `MRCONSO_UMLS` WHERE  `SAB` =  \"%s\"  AND `CODE` IN (" % id_convert[tag]
    else:
        query = "SELECT CUI, STR, LAT, ISPREF FROM `MRCONSO_UMLS` WHERE `SDUI` IN ("

    query += ",".join(['"' + x + '"' for x in id_list]) + ")"
    print query
    cur.execute(query)
    for query in cur.fetchall():
        res += [query]
    #print  tag  + " -> " + repr(id_list)

    return res
    


def get_links_from_DB():
    con = pymongo.Connection()
    db = con.wikipedia

    entries = list(db.npages.find({"pt_name": {"$ne": ""}}))
    links = []
    found = defaultdict(lambda : dict())
    nfound = {}
    for e in entries:
        if "infobox" in e:
            found[e["pt_name"]] = e["infobox"]
        else:
            nfound[e["pt_name"]] = e["en_name"].encode('utf-8')
    return found, nfound

if __name__ == '__main__':
    main()