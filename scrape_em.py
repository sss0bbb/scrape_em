#!/usr/bin/env python

#crawl through links on base url and save each:
#   event ID (tracking number)
#   cause (in tracking number page)
#   emissions (in tracking number page)
#save all to a csv at the end

import argparse
import requests
from bs4 import BeautifulSoup
from pprint import pprint
import re
import json
import csv

def parseArgs():
    parser = argparse.ArgumentParser(description='scrape a specific difficult site to make info more digestable')
    parser.add_argument('-u', action = "store", dest = 'url', required = True)
    return parser.parse_args()

def getFields(th_tags, t_event = False):
    colnames = []
    for th in th_tags:
        name = th.get_text().strip()
        #bad attempt at removing non-ascii char from tracking number colname
        #name = name.translate(None, '\xa0')
        colnames.append(name)
    #ugly manual fix for non-ascii chars in main event field name only
    if t_event:
        colnames[0] = u'TrackingNumber'
    return colnames

def getRow(td_tags, fields, t_event = False):
    fieldnum = 0
    row_dict = {}
    for td in td_tags:
        row_dict[fields[fieldnum]] = td.get_text().strip()
        if fields[fieldnum] == 'TrackingNumber':
            row_dict[u'URL'] = td.find('a', href = True)['href']
        fieldnum += 1
    return row_dict

def getTable(th_tags, tr_tags, t_event = False, t_emissions = False):
    all_rows = []
    fields = getFields(th_tags, t_event)
    for tr in tr_tags:
        td_tags = tr.find_all('td')
        new_row = getRow(td_tags, fields)
        all_rows.append(new_row)
    return all_rows

def getCause(event_soup):
    cause_header = event_soup.find('th', text = 'Cause')
    cause = cause_header.parent.find_next('td').get_text()
    return cause

def getEmissions(event, h3s):
    for h3 in h3s:
        s_fullname = h3.get_text()
        s_colon = s_fullname.find(':')
        s_name = s_fullname[:s_colon]
        event[s_name] = {}
        event[s_name][u'Fullname'] = s_fullname
        if h3.next_sibling.name == 'table':
            emission_th_tags = h3.next_sibling.find_all('th')
            emission_tr_tags = h3.next_sibling.find_all('tr')[1:]
            emission_list = getTable(emission_th_tags, emission_tr_tags, t_emissions = True)
            e_count = 1
            for e in emission_list:
                event[s_name][u'Emission ' + str(e_count)] = e
                e_count += 1


args = parseArgs()
base_url = args.url

base_page = requests.get(base_url)
base_soup = BeautifulSoup(base_page.content, 'html.parser')

#list(soup.children)[3] has all the main content
#html = list(soup.children)[3]

#list(html.children)[3] has everything in the <body>
#body = list(html.children)[3]

th_tags = base_soup.find_all('th')
tr_tags = base_soup.find_all('tr')
#dev setting to only deal with a few results. remove the following line for prod (910 links to follow... takes a very long time to follow each event link)
tr_tags = tr_tags[1:4]

#populate column names (keys) (fields) from table header values
event_fields = getFields(th_tags, t_event = True)

#store results in list of dicts?
#popular alternative seems to be pandas dataframes

events = getTable(th_tags, tr_tags, t_event = True)

#iterate through events and add extra info (cause, emissions sources and contaminants)
for event in events:
    #print event['URL']
    event_page = requests.get(event['URL'])
    event_soup = BeautifulSoup(event_page.content, 'html.parser')
    event[u'Cause'] = getCause(event_soup)
    h3_tags = event_soup.find_all('h3', text = re.compile("Source"))
    getEmissions(event, h3_tags)

pprint(events)






