import os
import scrapy
import sys
import shutil
import datetime
import json
import pandas as pd
from scrapy.spiders import CrawlSpider, Rule, SitemapSpider
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.exceptions import DropItem
from scrapy.linkextractors import LinkExtractor


filePath = os.path.dirname(os.path.realpath(__file__))
sys.path.append(filePath)

nahdiJson = filePath + r"\NahdiJson.json"

try:
    os.remove(nahdiJson)
except:
    pass

items = []


class NahdiItem(scrapy.Item):
    name = scrapy.Field()
    sku = scrapy.Field()
    price = scrapy.Field()
    finalPrice = scrapy.Field()
    promo = scrapy.Field()
    url = scrapy.Field()


class NahdiSpider(SitemapSpider):
    name = "Nahdi"
    allowed_domains = ["www.nahdionline.com"]

    sitemap_urls = [
        "https://www.nahdionline.com/media/sitemap_en-7-1.xml",
        "https://www.nahdionline.com/media/sitemap_en-7-2.xml",
        "https://www.nahdionline.com/media/sitemap_en-7-3.xml",
        "https://www.nahdionline.com/media/sitemap_en-7-4.xml",
    ]

    # class NahdiSpider(CrawlSpider):
    #     name = "Nahdi"
    #     allowed_domains = ["www.nahdionline.com"]

    # start_urls = ["https://www.nahdionline.com/en"]

    # rules = [
    #     Rule(
    #         LinkExtractor(allow="https://www.nahdionline.com/en/"),
    #         follow=True,
    #         callback="parse",
    #     )
    # ]

    custom_settings = {
        "FEED_URI": nahdiJson,
        "FEED_FORMAT": "json",  # "DEPTH_LIMIT": 2, # "CLOSESPIDER_PAGECOUNT": 150,# "FEED_EXPORT_FIELDS": ["name", "price"],
    }

    def parse(self, response):
        nahdiItem = NahdiItem()
        nahdiItem["sku"] = response.xpath(
            '//*[@class="product-info-main"]//*[@class="product-info-price"]/following-sibling::div/@data-sku'
        ).get()
        nahdiItem["name"] = response.xpath(
            '//*[@class="product-info-main"]//*[@data-ui-id="page-title-wrapper"]/text()'
        ).get()
        nahdiItem["price"] = response.xpath(
            '//*[@class="product-info-main"]//*[@data-price-type="oldPrice"]//*[@class="price"]/text()'
        ).get()
        nahdiItem["finalPrice"] = response.xpath(
            '//*[@class="product-info-main"]//*[@data-price-type="finalPrice"]//*[@class="price"]/text()'
        ).get()
        # nahdiItem["promo"] = response.xpath(
        #     '//*[@class="product-info-main"]//*[@class="nahdi-promo-sku"]/div/text()'
        # ).get()
        # nahdiItem["promo"] = response.xpath(
        #     '//*[@class="product-info-main"]//*[@class="nahdi-promo"]/text()'
        # ).get()
        # nahdiItem["promo"] = response.xpath(
        #     '//*[@id="ais-category-subtree"]//*[@class="nahdi-promo"]/text()'
        # ).get()
        nahdiItem["url"] = response.url
        items.append(
            {
                "sku": nahdiItem["sku"],
                "name": nahdiItem["name"],
                "price": nahdiItem["price"],
                "finalPrice": nahdiItem["finalPrice"],
                # "promo": nahdiItem["promo"],
                "url": nahdiItem["url"],
            }
        )
        return nahdiItem


crawler = CrawlerProcess(settings=get_project_settings())
crawler.crawl(NahdiSpider)
crawler.start()


def Nahdiprepare():
    nahdiOld = filePath + r"\NahdiOld.xlsx"  # nahdiNew = filePath + r"\NahdiNew.xlsx"
    nahdiChanges = filePath + r"\NahdiChanges.xlsx"
    nahdiBackup = filePath + r"\NahdiBackup.xlsx"

    dfNew = pd.DataFrame(pd.read_json(nahdiJson)).dropna(thresh=3).fillna(0)
    NewCoList = {"columns": [{"header": column} for column in dfNew.columns]}

    dfNewNoSku = dfNew[dfNew["sku"] == 0]
    dfNewNoSku.pop("sku")
    dfNewNoSku.drop_duplicates(["name"], inplace=True)
    dfNewNoSku.set_index("name", inplace=True)

    dfNew = dfNew[dfNew["sku"] != 0]
    dfNew.drop_duplicates(["sku"], inplace=True)
    dfNew.set_index("sku", inplace=True)

    dfOld = pd.DataFrame(pd.read_excel(nahdiOld)).fillna(0)

    dfOldNoSku = dfOld[dfOld["sku"] == 0]
    dfOldNoSku.pop("sku")
    dfOldNoSku.drop_duplicates(["name"], inplace=True)
    dfOldNoSku.set_index("name", inplace=True)

    dfOld = dfOld[dfOld["sku"] != 0]
    dfOld.drop_duplicates(["sku"], inplace=True)
    dfOld.set_index("sku", inplace=True)  # dfNew.set_index("sku", inplace=True)

    dfNahdiChanges = pd.DataFrame(pd.read_excel(nahdiChanges))
    dfNahdiChanges["Date"] = pd.to_datetime(dfNahdiChanges["Date"]).dt.date
    ChangesCoList = [{"header": column} for column in dfNahdiChanges.columns]
    dfNahdiChanges.set_index("EntryNo", inplace=True)

    for ind in dfNew.index:
        if ind in dfOld.index:
            # for c in range(dfNew.shape[1] - 1):
            for c in [0]:
                if dfNew.loc[ind][c] != dfOld.loc[ind][c]:
                    dfNahdiChanges.loc[len(dfNahdiChanges.index)] = [
                        ind,
                        dfNew.loc[ind][0],
                        dfNew.columns[c],
                        dfOld.loc[ind][c],
                        dfNew.loc[ind][c],
                        datetime.datetime.today().date(),
                        datetime.datetime.today().time().strftime("%I %p"),
                    ]
            finalPriceChange = dfOld.loc[ind][2] != dfNew.loc[ind][2]
            oldPriceZero = dfOld.loc[ind][1] == 0
            newPriceZero = dfNew.loc[ind][1] == 0
            if finalPriceChange:
                if oldPriceZero and newPriceZero:
                    priceOfferStatus = "PriceChanged"
                elif oldPriceZero and not newPriceZero:
                    priceOfferStatus = "OfferAdded"
                elif not oldPriceZero and newPriceZero:
                    priceOfferStatus = "OfferRemoved"
                elif not oldPriceZero and not newPriceZero:
                    if float(dfOld.loc[ind][2][:-4].replace(",", "")) > float(
                        dfNew.loc[ind][2][:-4].replace(",", "")
                    ):
                        priceOfferStatus = "OfferImproved"
                    else:
                        priceOfferStatus = "OfferWeakened"
                dfNahdiChanges.loc[len(dfNahdiChanges.index)] = [
                    ind,
                    dfNew.loc[ind][0],
                    priceOfferStatus,
                    dfOld.loc[ind][2],
                    dfNew.loc[ind][2],
                    datetime.datetime.today().date(),
                    datetime.datetime.today().time().strftime("%I %p"),
                ]
        else:
            # if (dfOld[dfOld["name"] == dfNew.loc[ind][0]]).empty:
            dfNahdiChanges.loc[len(dfNahdiChanges.index)] = [
                ind,
                dfNew.loc[ind][0],
                "Added",
                "",
                "",
                datetime.datetime.today().date(),
                datetime.datetime.today().time().strftime("%I %p"),
            ]

    for ind in dfOld.index:
        if ind in dfNew.index:
            pass
        else:
            dfNew.loc[ind] = dfOld.loc[ind]

    for ind in dfNewNoSku.index:
        if ind in dfOldNoSku.index:
            finalPriceChange = dfOldNoSku.loc[ind][1] != dfNewNoSku.loc[ind][1]
            oldPriceZero = dfOldNoSku.loc[ind][0] == 0
            newPriceZero = dfNewNoSku.loc[ind][0] == 0
            if finalPriceChange:
                if oldPriceZero and newPriceZero:
                    priceOfferStatus = "PriceChanged"
                elif oldPriceZero and not newPriceZero:
                    priceOfferStatus = "OfferAdded"
                elif not oldPriceZero and newPriceZero:
                    priceOfferStatus = "OfferRemoved"
                elif not oldPriceZero and not newPriceZero:
                    if float(dfOldNoSku.loc[ind][1][:-4].replace(",", "")) > float(
                        dfNewNoSku.loc[ind][1][:-4].replace(",", "")
                    ):
                        priceOfferStatus = "OfferImproved"
                    else:
                        priceOfferStatus = "OfferWeakened"
                dfNahdiChanges.loc[len(dfNahdiChanges.index)] = [
                    "",
                    ind,
                    priceOfferStatus,
                    dfOldNoSku.loc[ind][1],
                    dfNewNoSku.loc[ind][1],
                    datetime.datetime.today().date(),
                    datetime.datetime.today().time().strftime("%I %p"),
                ]
        else:
            dfNahdiChanges.loc[len(dfNahdiChanges.index)] = [
                "",
                ind,
                "Added",
                "",
                "",
                datetime.datetime.today().date(),
                datetime.datetime.today().time().strftime("%I %p"),
            ]

    for ind in dfOldNoSku.index:
        if ind in dfNewNoSku.index:
            pass
        else:
            dfNewNoSku.loc[ind] = dfOldNoSku.loc[ind]

    fileChanges = filePath + r"\NahdiChanges.xlsx"

    dfNahdiChanges.sort_values(["Date", "Change", "Name"], inplace=True)
    with pd.ExcelWriter(fileChanges, engine="xlsxwriter") as writer:
        dfNahdiChanges.to_excel(writer)
        ws = writer.sheets["Sheet1"]
        ws.add_table(
            0,
            0,
            dfNahdiChanges.shape[0],
            dfNahdiChanges.shape[1],
            {"columns": ChangesCoList},
        )
        ws.set_column(0, 0, 10)
        for idx, col in enumerate(dfNahdiChanges.columns):
            series = dfNahdiChanges[col]
            maxLen = max(series.astype(str).map(len).max(), len(str(series.name)) + 2)
            maxLen = 45 if maxLen > 45 else maxLen
            ws.set_column(idx + 1, idx + 1, maxLen)

    shutil.copy(nahdiOld, nahdiBackup)

    dfNew.reset_index(inplace=True)
    dfNewNoSku.reset_index(inplace=True)
    dfNewNoSku["sku"] = 0
    dfNewNoSku = dfNewNoSku[["sku", "name", "price", "finalPrice", "url"]]
    dfNew = pd.concat([dfNew, dfNewNoSku])

    with pd.ExcelWriter(nahdiOld, engine="xlsxwriter") as writer:
        dfNew.to_excel(writer, index=False)
        ws = writer.sheets["Sheet1"]
        ws.add_table(
            0,
            0,
            dfNew.shape[0],
            dfNew.shape[1] - 1,
            NewCoList,
        )
        # ws.set_column(0, 0, 10)
        for idx, col in enumerate(dfNew.columns):
            series = dfNew[col]
            maxLen = max(series.astype(str).map(len).max(), len(str(series.name)) + 2)
            maxLen = 45 if maxLen > 45 else maxLen
            ws.set_column(idx, idx, maxLen)


Nahdiprepare()
