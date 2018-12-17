import json
import time
import argparse
import unicodecsv as csv
import sys
import os
import math
from lxml import html, etree
import re
import requests
import bs4
import pandas as pd
import numpy as np

'''
This is a modified version of the scraper found at https://www.scrapehero.com/how-to-scrape-job-listings-from-glassdoor-using-python-and-lxml/

Modifications include going through each job posting one by one as the solution at scrapehero didn't work currently

'''


def get_job_posts(keyword, place):
    '''
    Inputs :
    keyword : position title.
    place : city to search.

    Function goes through job posts on glassdoor and scrapes the title, location, salary, rating, and job description.
    '''
    # Checking if keyword is a list of positions or just a single position. If keyword is single position convert to a list to be iterated over.

    if type(keyword) is not str:
        raise ValueError('Please insert string for position title')

    print("Searching Glassdoor for ", keyword, "openings in", place)
    keyword = keyword.replace(" ", "'+'")

    '''
    Step 1 :
    Get the location_ids for the city.
    '''

    # Create the header for the requests required to get location_id
    location_headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.01',
        'accept-encoding': 'gzip, deflate, sdch, br',
        'accept-language': 'en-GB,en-US;q=0.8,en;q=0.6',
        'referer': 'https://www.glassdoor.com/',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/51.0.2704.79 Chrome/51.0.2704.79 Safari/537.36',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive'
    }

    # Set header to get job postings
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'accept-encoding': 'gzip, deflate, sdch, br',
        'accept-language': 'en-GB,en-US;q=0.8,en;q=0.6',
        'referer': 'https://www.glassdoor.com/',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/51.0.2704.79 Chrome/51.0.2704.79 Safari/537.36',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive'
    }

    # Set data to be passed into URL.
    data = {"term": place,
            "maxLocationsToReturn": 10}

    location_url = "https://www.glassdoor.co.in/findPopularLocationAjax.htm?"

    print("Fetching location details")

    # Send request to glassdoor to get the place_id to include in our job posting search.

    location_response = requests.post(
        location_url, headers=location_headers, data=data).json()
    place_id = location_response[0]['locationId']

    '''
    Step 2: Using keyword and place_id loop over job postings and grab the Company Name, Job Title, Location, Salary, Rating, and Job Description
    '''

    job_litsting_url = 'https://www.glassdoor.com/Job/jobs.htm'

    data = {
        'clickSource': 'searchBtn',
        'sc.keyword': keyword,
        'locT': 'C',
        'locId': place_id,
        'jobType': ''
    }

    # Generating the base request to grab the Next button link. This is needed to grab all the pages of the search.
    response = requests.post(job_litsting_url, headers=headers, data=data)
    parser = html.fromstring(response.text)
    base_url = "https://www.glassdoor.com"
    parser.make_links_absolute(base_url)

    # Set the path to get the next link (The next page on the glassdoor job search)
    XPATH_GETNEXT = '//li[@class="next"]//a/@href'

    next_link = parser.xpath(XPATH_GETNEXT)[0]

    # Get the first link by replacing the ending IP2 (page 2) with IP1 for page 1.
    first_link = list(next_link)
    first_link[-5] = '1'
    first_link = "".join(first_link)

    # Create a list of all the links to be looped over to grab the job posting information
    job_pages = []
    job_pages.append(first_link)
    job_pages.append(next_link)

    # Loop over until we can no longer find the next page.
    response = requests.post(next_link, headers=headers)
    while response.status_code == 200:
        parser = html.fromstring(response.text)
        parser.make_links_absolute(base_url)
        try:
            next_link = parser.xpath(XPATH_GETNEXT)[0]
        except:
            break
        job_pages.append(next_link)
        response = requests.post(next_link, headers=headers)

    print('Total job pages found:', len(job_pages))

    job_listings = []
    # Each job description has its own page so we will Loop over each job listing on each of the job page. Append these job listing urls to a list.
    for job_page in job_pages:
        XPATH_JOBURL = './/div[@class="flexbox jobTitle"]//a/@href'
        response = requests.post(job_page, headers=headers)
        parser = html.fromstring(response.text)
        parser.make_links_absolute(base_url)
        job_listings.append(list(parser.xpath(XPATH_JOBURL)))

    # Flatten the list out to be looped over.
    job_listings = [job for listings in job_listings for job in listings]

    print('Total job listings found:', len(job_listings))

    # Defining the XPATHS for each feature we will scrape

    XPATH_COMPANY = './/span[@class="strong ib"]/text()'
    XPATH_JOBTITLE = './/h2[@class="noMargTop margBotXs strong"]/text()'
    XPATH_LOC = './/span[@class="subtle ib"]/text()'
    XPATH_RATING = './/span[@class="compactStars margRtSm"]/text()'
    XPATH_SALARY = './/h2[@class="salEst"]/text()'
    XPATH_JOB_DESC = './/section[@id="JobDetailsInfo"]//text()'

    # Some fields will be null such as Rating and Salary. So this function attempts to find the field and if fails return NULL.
    def try_field(s):
        try:
            return s[0][1:]
        except:
            return(np.nan)

    # Create Dataframe to add Jobs to
    columns = ["Company", "Job Title", "Location",
               "Rating", "Salary", "Description"]

    # Define database to return results in
    job_df = pd.DataFrame(columns=columns)

    # Loop over each job listings. Grab Company, Job Title, Location, Rating, Salary, and Description
    for i, job in enumerate(job_listings):
        response = requests.post(job, headers=headers)
        parser = html.fromstring(response.text)
        parser.make_links_absolute(base_url)
        job_df.loc[i, ["Company", "Job Title", "Location", "Rating", "Salary", "Description"]] = \
            [parser.xpath(XPATH_COMPANY),
             parser.xpath(XPATH_JOBTITLE)[0],
             parser.xpath(XPATH_LOC)[0][3:],
             try_field(XPATH_RATING),
             try_field(parser.xpath(XPATH_SALARY)),
             " ".join(list(parser.xpath(XPATH_JOB_DESC)))[4:]]
        # Adding Sleep Timer to not flood servers
        time.sleep(1)

    job_df['Company'] = job_df['Company'].apply(lambda x: x[0])

    return job_df
