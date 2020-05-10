# coding: utf-8

import requests
import re
import pandas as pd
import numpy as np

from lxml import html
from scrapy.selector import Selector
from multiprocessing import Process

# Change start url of country. In this project, I crawled restaurants from Turkey.

country_url = 'https://www.tripadvisor.com.tr/Restaurants-g293969-Turkey.html'

# To speed up crawling process, I split the whole process into sub-processes. However It uses more CPU.
# 10 process is ideal for 4 cores. If you have less, you can set it to 5.
process_count = 10


def crawl_all_pages(country_url):

    raw_html = requests.get(country_url)

    tree = html.fromstring(raw_html.content)
    link_list = tree.xpath("//div[@class='geo_name']/a/@href")

    # pagination_link_list is to find how many pages that country has

    pagination_link_list = tree.xpath("//div[@class='pageNumbers']/a/@data-page-number")

    # In tripadvisor pagination system, last item of the list gives maximum pagination number
    # total_pagination will be iteration number for country crawling

    total_pagination = pagination_link_list[-1]

    country_pagination_links = []

    # Tripadvisor link structure is: https://www.tripadvisor.com + /Restaurants-g293974 + oa20 + City.html
    # Therefore I split link structure with '-' and construct with 20 pagination after 'oa'

    for page in range(1, int(total_pagination)):
        country_pagination_links.append(
            '{}-{}-{}-{}'.format(country_url.split('-')[0], country_url.split('-')[1], 'oa{}'.format(page * 20),
                                 country_url.split('-')[2]))

    for pagination_link in country_pagination_links:
        temp_html = requests.get(pagination_link)
        temp_tree = html.fromstring(temp_html.content)
        temp_list = temp_tree.xpath("//ul[@class='geoList']/li/a/@href")
        link_list = link_list + temp_list

    # return of this function is total start urls list of all cities.
    return link_list


def crawling_cities(link_list):

    restaurant_urls = []
    # restaurant_urls = restaurant_urls + urls

    for city_link in link_list:
        city_start_link = 'https://www.tripadvisor.com' + str(city_link)
        temp_city_html = requests.get(city_start_link)
        temp_tree = html.fromstring(temp_city_html.content)
        city_pagination_link_list = temp_tree.xpath("//div[@class='pageNumbers']/a/@data-page-number")

        temp_urls = re.findall('/Restaurant_Review-g.*?.html', temp_city_html.text)
        restaurant_urls = restaurant_urls + temp_urls

        if len(city_pagination_link_list) == 0:
            continue

        city_total_pagination = city_pagination_link_list[-1]
        city_total_pagination = int(city_total_pagination)

        # starting pagination of city

        for page in range(1, city_total_pagination):
            page_to_crawl = '{}-{}-{}-{}'.format(city_start_link.split('-')[0], city_start_link.split('-')[1],
                                                 'oa{}'.format(page * 30), city_start_link.split('-')[2])
            temp_city_html = requests.get(page_to_crawl)
            temp_urls = re.findall('/Restaurant_Review-g.*?.html', temp_city_html.text)
            restaurant_urls = restaurant_urls + temp_urls
            print(page_to_crawl)

    # Crawlers may get a restaurant more than one time. So I am dropping duplicates.
    print(len(restaurant_urls))
    restaurant_urls = list(dict.fromkeys(restaurant_urls))

    # Function returns all restaurant urls.

    return restaurant_urls


def crawl_restaurant(link):

    # This function crawls some necessary information of restaurants. If you need more detail from restaurant,
    # modify this area. You should need to know about xpath to extract information from web sites.

    restaurant_name_list = []
    review_count_list = []
    price_range_list = []
    adress_list = []
    phone_list = []
    average_rating_list = []
    kitchen_types_list = []
    latitude_list = []
    longtitude_list = []
    url_list = []

    #xpath rules to extract data from restaurant page

    restaurant_name_xpath = "normalize-space(//h1[@class='ui_header h1'])"
    review_count_xpath = "normalize-space(//a[@class='restaurants-detail-overview-cards-RatingsOverviewCard__ratingCount--DFxkG'])"
    price_range_xpath = "normalize-space(//div[@class='restaurants-detail-overview-cards-DetailsSectionOverviewCard__tagText--1OH6h'])"
    adress_xpath = "normalize-space(//span[@class='detail '])"
    phone_xpath = "normalize-space(//span[@class='detail  is-hidden-mobile'])"
    average_rating_xpath = "normalize-space(//span[@class='restaurants-detail-overview-cards-RatingsOverviewCard__overallRating--nohTl'])"
    kitchen_types_xpath = "normalize-space(//div[@class='restaurants-detail-overview-cards-DetailsSectionOverviewCard__detailsSummary--evhlS']/div[2]/div[2])"

    page = requests.get(link)

    restaurant_name = Selector(text=page.text).xpath(restaurant_name_xpath).extract()[0]
    restaurant_name_list.append(restaurant_name)

    try:
        review_count = Selector(text=page.text).xpath(review_count_xpath).extract()[0]
        review_count_list.append(review_count)
    except:
        review_count_list.append(np.nan)
    ####
    try:
        price_range = Selector(text=page.text).xpath(price_range_xpath).extract()[0]
        price_range_list.append(price_range)
    except:
        price_range_list.append(np.nan)
    ####
    try:
        adress = Selector(text=page.text).xpath(adress_xpath).extract()[0]
        adress_list.append(adress)
    except:
        adress_list.append(np.nan)
    ####
    try:
        phone = Selector(text=page.text).xpath(phone_xpath).extract()[0]
        phone_list.append(phone)
    except:
        phone_list.append(np.nan)
    ####
    try:
        average_rating = Selector(text=page.text).xpath(average_rating_xpath).extract()[0]
        average_rating_list.append(average_rating)
    except:
        average_rating_list.append(np.nan)
    ####
    try:
        kitchen_types = Selector(text=page.text).xpath(kitchen_types_xpath).extract()[0]
        kitchen_types_list.append(kitchen_types)
    except:
        kitchen_types_list.append(np.nan)
    try:
        latitude = re.findall(r'\"latitude\":\"(.+?)\"', page.text)
        latitude_list.append(latitude[0])
    except:
        latitude_list.append(np.nan)
    ####
    try:
        longtitude = re.findall(r'\"longitude\":\"(.+?)\"', page.text)
        longtitude_list.append(longtitude[0])
    except:
        longtitude_list.append(np.nan)

    ####
    try:
        url_list.append(link)
    except:
        url_list.append(np.nan)

    # Making a dataframe using pandas

    d = {'restaurantName': restaurant_name_list,
         'reviewCount': review_count_list,
         'priceRange': price_range_list,
         'adres': adress_list,
         'phone': phone_list,
         'averageRating': average_rating_list,
         'kitchenTypes': kitchen_types_list,
         'latitude': latitude_list,
         'longtitude': longtitude_list,
         'url': url_list
         }

    # Appending this dataframe to csv file

    df = pd.DataFrame(data=d)
    df.to_csv('tripadvisor_restaurants.csv', mode='a', header=False, index=None)
    # print("%s Crawled" % restaurant_name)


def crawler(url_list, part_count):

    offset_count = int(len(url_list) / 10)

    actual_list = url_list[part_count*offset_count:(part_count+1)*offset_count]

    for url in actual_list:
        full_url = 'https://www.tripadvisor.com' + url
        crawl_restaurant(full_url)


if __name__ == '__main__':
    country_link_list = crawl_all_pages(country_url)

    restaurant_urls = crawling_cities(country_link_list)

    print(len(restaurant_urls))

    jobs = []

    for split in range(process_count):
        p = Process(target=crawler, args=(restaurant_urls, split))
        p.daemon = True
        jobs.append(p)
        p.start()

    for job in jobs:
        job.join()




