# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class BaseItem(scrapy.Item):

    def db_keys(self):
        for k in self.keys():
            if self.fields[k].get('db'):
                yield k

    def db_items(self):
        for k, v in self.items():
            if self.fields[k].get('db'):
                yield (k, v)

class ProductSpiderItem(BaseItem):
    mall_id = scrapy.Field()
    item_id = scrapy.Field(db=1)
    title = scrapy.Field(db=1)
    price = scrapy.Field(db=1)
    original_price = scrapy.Field(db=1)
    url = scrapy.Field(db=1)
    image_url = scrapy.Field(db=1)
    status = scrapy.Field(db=1)
    cashback_cat_id = scrapy.Field(db=1)
    updated_time = scrapy.Field(db=1)
    hot_status = scrapy.Field(db=1)
    shop_name = scrapy.Field(db=1)
    model_name = scrapy.Field(db=1)
    promotion_activity = scrapy.Field(db=1)

    cat_1_raw_id = scrapy.Field()
    cat_2_raw_id = scrapy.Field()
    cat_3_raw_id = scrapy.Field()

    source_type = scrapy.Field()
    spider_label = scrapy.Field()

    def __init__(self, response):
        super().__init__()
        source_type = response.meta.get('source_type')
        if source_type:
            self['source_type'] = source_type
        spider_label = response.meta.get('spider_label')
        if spider_label:
            self['spider_label'] = spider_label

class ProductLastSeenItem(BaseItem):
    item_id = scrapy.Field(db=1)
    lastseen_time = scrapy.Field(db=1)

class ProductCatItem(BaseItem):
    mall_id = scrapy.Field()
    item_id = scrapy.Field(db=1)
    cat_1_raw_id = scrapy.Field(db=1)
    cat_1_title = scrapy.Field(db=1)
    cat_2_raw_id = scrapy.Field(db=1)
    cat_2_title = scrapy.Field(db=1)
    cat_3_raw_id = scrapy.Field(db=1)
    cat_3_title = scrapy.Field(db=1)
    other_cats_raw_id = scrapy.Field(db=1)
    other_cats_title = scrapy.Field(db=1)
    updated_time = scrapy.Field(db=1)

class ProductBrandKeywordItem(BaseItem):
    mall_id = scrapy.Field(db=1)
    feat_id = scrapy.Field(db=1)
    item_id = scrapy.Field(db=1)

class ProductGymCatItem(BaseItem):
    gym_cat_id = scrapy.Field(db=1)
    item_id = scrapy.Field(db=1)

class SectionGoodsItem(BaseItem):
    mall_id = scrapy.Field(db=1)
    item_id = scrapy.Field(db=1)
    section_id = scrapy.Field(db=1)

class PromotionGoodsItem(BaseItem):
    mall_id = scrapy.Field(db=1)
    id = scrapy.Field(db=1)
    content = scrapy.Field(db=1)
    image_url = scrapy.Field(db=1)
    landing_url = scrapy.Field(db=1)

class RawKeywordItem(BaseItem):
    id = scrapy.Field(db=1)
    word = scrapy.Field(db=1)
    source = scrapy.Field(db=1)
    source_url = scrapy.Field(db=1)
    word_type = scrapy.Field(db=1)
    create_time = scrapy.Field(db=1)
    