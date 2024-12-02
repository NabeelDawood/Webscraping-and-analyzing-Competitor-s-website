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

competitorJson = filePath + r"\competitorJson.json"

try:
    os.remove(competitorJson)
except:
    pass

items = []


class CompetitorItem(scrapy.Item):
    name = scrapy.Field()
    sku = scrapy.Field()
    price = scrapy.Field()
    finalPrice = scrapy.Field()
    promo = scrapy.Field()
    url = scrapy.Field()


class CompetitorSpider(SitemapSpider):
    name = "Competitor"
    allowed_domains = ["www.competitoronline.com"]

    sitemap_urls = [
        "https://www.competitoronline.com/media/sitemap_en-7-1.xml",
        "https://www.competitoronline.com/media/sitemap_en-7-2.xml",
        "https://www.competitoronline.com/media/sitemap_en-7-3.xml",
        "https://www.competitoronline.com/media/sitemap_en-7-4.xml",
    ]

    # class CompetitorSpider(CrawlSpider):
    #     name = "Competitor"
    #     allowed_domains = ["www.competitoronline.com"]

    # start_urls = ["https://www.competitoronline.com/en"]

    # rules = [
    #     Rule(
    #         LinkExtractor(allow="https://www.competitoronline.com/en/"),
    #         follow=True,
    #         callback="parse",
    #     )
    # ]

    custom_settings = {
        "FEED_URI": competitorJson,
        "FEED_FORMAT": "json",  # "DEPTH_LIMIT": 2, # "CLOSESPIDER_PAGECOUNT": 150,# "FEED_EXPORT_FIELDS": ["name", "price"],
    }

    def parse(self, response):
        competitorItem = CompetitorItem()
        competitorItem["sku"] = response.xpath(
            '//*[@class="product-info-main"]//*[@class="product-info-price"]/following-sibling::div/@data-sku'
        ).get()
        competitorItem["name"] = response.xpath(
            '//*[@class="product-info-main"]//*[@data-ui-id="page-title-wrapper"]/text()'
        ).get()
        competitorItem["price"] = response.xpath(
            '//*[@class="product-info-main"]//*[@data-price-type="oldPrice"]//*[@class="price"]/text()'
        ).get()
        competitorItem["finalPrice"] = response.xpath(
            '//*[@class="product-info-main"]//*[@data-price-type="finalPrice"]//*[@class="price"]/text()'
        ).get()
        # competitorItem["promo"] = response.xpath(
        #     '//*[@class="product-info-main"]//*[@class="competitor-promo-sku"]/div/text()'
        # ).get()
        # competitorItem["promo"] = response.xpath(
        #     '//*[@class="product-info-main"]//*[@class="competitor-promo"]/text()'
        # ).get()
        # competitorItem["promo"] = response.xpath(
        #     '//*[@id="ais-category-subtree"]//*[@class="competitor-promo"]/text()'
        # ).get()
        competitorItem["url"] = response.url
        items.append(
            {
                "sku": competitorItem["sku"],
                "name": competitorItem["name"],
                "price": competitorItem["price"],
                "finalPrice": competitorItem["finalPrice"],
                # "promo": competitorItem["promo"],
                "url": competitorItem["url"],
            }
        )
        return competitorItem


crawler = CrawlerProcess(settings=get_project_settings())
crawler.crawl(CompetitorSpider)
crawler.start()


def Competitorprepare():
    competitorOld = filePath + r"\CompetitorOld.xlsx"  # competitorNew = filePath + r"\CompetitorNew.xlsx"
    competitorChanges = filePath + r"\CompetitorChanges.xlsx"
    competitorBackup = filePath + r"\CompetitorBackup.xlsx"

    dfNew = pd.DataFrame(pd.read_json(competitorJson)).dropna(thresh=3).fillna(0)
    NewCoList = {"columns": [{"header": column} for column in dfNew.columns]}

    dfNewNoSku = dfNew[dfNew["sku"] == 0]
    dfNewNoSku.pop("sku")
    dfNewNoSku.drop_duplicates(["name"], inplace=True)
    dfNewNoSku.set_index("name", inplace=True)

    dfNew = dfNew[dfNew["sku"] != 0]
    dfNew.drop_duplicates(["sku"], inplace=True)
    dfNew.set_index("sku", inplace=True)

    dfOld = pd.DataFrame(pd.read_excel(competitorOld)).fillna(0)

    dfOldNoSku = dfOld[dfOld["sku"] == 0]
    dfOldNoSku.pop("sku")
    dfOldNoSku.drop_duplicates(["name"], inplace=True)
    dfOldNoSku.set_index("name", inplace=True)

    dfOld = dfOld[dfOld["sku"] != 0]
    dfOld.drop_duplicates(["sku"], inplace=True)
    dfOld.set_index("sku", inplace=True)  # dfNew.set_index("sku", inplace=True)

    dfCompetitorChanges = pd.DataFrame(pd.read_excel(competitorChanges))
    dfCompetitorChanges["Date"] = pd.to_datetime(dfCompetitorChanges["Date"]).dt.date
    ChangesCoList = [{"header": column} for column in dfCompetitorChanges.columns]
    dfCompetitorChanges.set_index("EntryNo", inplace=True)

    for ind in dfNew.index:
        if ind in dfOld.index:
            # for c in range(dfNew.shape[1] - 1):
            for c in [0]:
                if dfNew.loc[ind][c] != dfOld.loc[ind][c]:
                    dfCompetitorChanges.loc[len(dfCompetitorChanges.index)] = [
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
                dfCompetitorChanges.loc[len(dfCompetitorChanges.index)] = [
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
            dfCompetitorChanges.loc[len(dfCompetitorChanges.index)] = [
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
                dfCompetitorChanges.loc[len(dfCompetitorChanges.index)] = [
                    "",
                    ind,
                    priceOfferStatus,
                    dfOldNoSku.loc[ind][1],
                    dfNewNoSku.loc[ind][1],
                    datetime.datetime.today().date(),
                    datetime.datetime.today().time().strftime("%I %p"),
                ]
        else:
            dfCompetitorChanges.loc[len(dfCompetitorChanges.index)] = [
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

    fileChanges = filePath + r"\CompetitorChanges.xlsx"

    dfCompetitorChanges.sort_values(["Date", "Change", "Name"], inplace=True)
    with pd.ExcelWriter(fileChanges, engine="xlsxwriter") as writer:
        dfCompetitorChanges.to_excel(writer)
        ws = writer.sheets["Sheet1"]
        ws.add_table(
            0,
            0,
            dfCompetitorChanges.shape[0],
            dfCompetitorChanges.shape[1],
            {"columns": ChangesCoList},
        )
        ws.set_column(0, 0, 10)
        for idx, col in enumerate(dfCompetitorChanges.columns):
            series = dfCompetitorChanges[col]
            maxLen = max(series.astype(str).map(len).max(), len(str(series.name)) + 2)
            maxLen = 45 if maxLen > 45 else maxLen
            ws.set_column(idx + 1, idx + 1, maxLen)

    shutil.copy(competitorOld, competitorBackup)

    dfNew.reset_index(inplace=True)
    dfNewNoSku.reset_index(inplace=True)
    dfNewNoSku["sku"] = 0
    dfNewNoSku = dfNewNoSku[["sku", "name", "price", "finalPrice", "url"]]
    dfNew = pd.concat([dfNew, dfNewNoSku])

    with pd.ExcelWriter(competitorOld, engine="xlsxwriter") as writer:
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


Competitorprepare()
