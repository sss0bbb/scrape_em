#!/usr/bin/env python

#crawl through links on base url and save each:
#   event ID (tracking number)
#   cause (in tracking number page)
#   emissions (in tracking number page)
#save all to a csv at the end

#recursive json to csv parsing adapted from https://github.com/vinay20045/json-to-csv

import argparse
import requests
from bs4 import BeautifulSoup
from pprint import pprint
import re
import json
import csv


def parseArgs():
    parser = argparse.ArgumentParser(description = 'scrape a specific difficult site to make info more digestable')
    parser.add_argument('-u', '--url', action = 'store', dest = 'url', required = True, help = 'required base url')
    parser.add_argument('-m', '--max', action = 'store', dest = 'max_events', type = int, default = 0, help = 'great for limiting results for testing purposes')
    parser.add_argument('-c', '--csv', action = 'store', dest = 'csv', help = 'specify csv file base name - two files will be created with this base name.')
    parser.add_argument('-l', '--load', action = 'store', dest = 'json', help = 'specify json file to load previous results from and save new things to')
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
        colnames[0] = u'TrackingNum'
    return colnames

def getRow(td_tags, fields, t_event = False):
    fieldnum = 0
    row_dict = {}
    for td in td_tags:
        row_dict[fields[fieldnum]] = td.get_text().strip()
        if fields[fieldnum] == 'TrackingNum':
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

def getEmission(tracknum, h3s, emissions):
    #loop through emissions labeled as "Source" in h3 tags followed by tables of contaminants
    #all storage should be in the emissions list of dicts
    emission = {}
    for h3 in h3s:
        if h3.next_sibling.name == 'table':
            s_fullname = h3.get_text()
            s_colon = s_fullname.find(':')
            s_name = s_fullname[:s_colon]
            emission[u'TrackingNum'] = tracknum
            #event[s_name] = {}
            emission[u'Name'] = s_name
            emission[u'Fullname'] = s_fullname
            emission_th_tags = h3.next_sibling.find_all('th')
            emission_tr_tags = h3.next_sibling.find_all('tr')[1:]
            emission_list = getTable(emission_th_tags, emission_tr_tags, t_emissions = True)
            #pprint(emission_list)
            #e_count = 1
            for e in emission_list:
                emission.update(e)
                #e_count += 1
            pprint(emission)
            emissions.append(emission)

def getAllEmissions(events, emissions):
    for event in events:
        #pprint(event)
        event_page = requests.get(event['URL'])
        event_soup = BeautifulSoup(event_page.content, 'html.parser')
        event[u'Cause'] = getCause(event_soup)
        h3_tags = event_soup.find_all('h3', text = re.compile("Source"))
        getEmission(event['TrackingNum'], h3_tags, emissions)

def writeCSV(ldicts, csvfile, t_event = False, t_emissions = False):
    #get file extension
    csv_basename = csvfile
    if csvfile.rfind('.') > 0:
        csv_basename = csvfile[:csvfile.rfind('.')]
    if t_event:
        csvfile = csv_basename + '_events.csv'
        #suggested
        #header = [u'TrackingNum', u'Regulated Entity Number', u'RNNum', u'City', u'County', u'Type', u'Event Begin', u'Event End', u'Basis', u'Cause', u'Action Taken', u'Estimation Method']
        header = [u'TrackingNum', u'Type', u'Status', u'Cause', u'Began', u'Ended', u'URL']
        #todo: compare header to ldicts[0] to make sure that the contents are the same even if they're in a different order
        #   ValueError thrown when header doesn't match - use try instead
    elif t_emissions:
        csvfile = csv_basename + '_emissions.csv'
        header = [u'TrackingNum', u'Name', u'Fullname', u'Contaminant', u'Authorization', u'Limit', u'Amount Released']
    #pprint(ldicts[0])
    #print 'key differences:'
    #print set(header) - set(ldicts[0])
    with open(csvfile, 'w') as f:
        writer = csv.DictWriter(f, header, quoting = csv.QUOTE_ALL)
        writer.writeheader()
        for row in ldicts:
            writer.writerow(row)

def getTNlist(events):
    tnlist = []
    for event in events:
        tnlist.append(event['TrackingNum'])
    return tnlist

def getuniqueTNs(tnlist1, tnlist2):
    uniquetns = []
    for tn in tnlist1:
        if tn in tnlist2:
            uniquetns.append(tn)
    return uniquetns

def getUniqueNewEvents(de_events, new_events):
    u_new_events = []
    de_tnlist = getTNlist(de_events)
    ne_tnlist = getTNlist(new_events)
    for event in new_events:
        if event['TrackingNum'] not in de_tnlist:
            u_new_events.append(event)
    return u_new_events

def openJsonFile(data, filename = False):
    if filename:
        try:
            with open(filename, 'r') as f:
                json_data = json.load(f)
                data.update(json_data)
            print 'loaded json cache successfully:', len(data['events']), 'events -', len(data['emissions']), 'emissions'
        except IOError:
            print 'json file \"', filename, '\" does not exist. starting with no cached data. will create json cache file and save data there.'
            data['events'] = []
            data['emissions'] = []

def writeJsonFile(data, filename = False):
    if filename:
        print 'attempting to update json cache file'
        with open(filename, 'w') as f:
            json.dump(data, f)


def main():
    args = parseArgs()
    base_url = args.url

    base_page = requests.get(base_url)
    base_soup = BeautifulSoup(base_page.content, 'html.parser')

    #instead of this we might just want to search for the first table, then look for the th and tr tags in that first table only
    th_tags = base_soup.find_all('th')
    tr_tags = base_soup.find_all('tr')
    #dev setting to only deal with a few results. remove the following line for prod (910 links to follow... takes a very long time to follow each event link)
    print 'max_events is:', args.max_events
    if args.max_events > 0:
        tr_tags = tr_tags[1:args.max_events + 1]
    else:
        tr_tags = tr_tags[1:]

    #initialize main data storage, try to load cached data from json file, then get new stuff from the site
    data = {}

    #try to open json file
    openJsonFile(data, args.json)

    #store events in list of dicts
    #get this full list no matter the cached event data. true time cost is getting emission data, not events
    print 'grabbing all events from base url'
    all_events = getTable(th_tags, tr_tags, t_event = True)

    #find new events by tracking number that don't exist in the loaded json events data
    print 'finding new events based on saved cache data'
    new_events = getUniqueNewEvents(data['events'], all_events)

    #store emissions in their own list of dicts
    new_emissions = []
    #iterate through events and grab extra info (cause, emissions sources and contaminants)
    print 'grabbing new emissions data based on new events'
    getAllEmissions(new_events, new_emissions)

    #merge all new data into main stored data
    data['events'] = data['events'] + new_events
    data['emissions'] = data['emissions'] + new_emissions

    #write all data to json cache file
    writeJsonFile(data, args.json)

    #final output - either csv or a pprint example
    if args.csv:
        print 'writing all data to CSVs'
        writeCSV(data['events'], args.csv, t_event = True)
        writeCSV(data['emissions'], args.csv, t_emissions = True)
    else:
        print 'no CSV file given... printing first 3 events as a sample'
        pprint(data['events'][:3])

    print 'DONE!'

if __name__== "__main__":
    main()

