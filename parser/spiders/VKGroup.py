import scrapy
import json
import logging
import os
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.DEBUG)

class VkGroupSpider(scrapy.Spider):
    name = 'VKgroup'

    def __init__(self, *args, **kwargs):
        self.api_url = "https://api.vk.com"
        self.group_id = kwargs['group']
        self.api_version = 5.131
        self.access_token = os.getenv("TOKEN")
        self.group_fields = kwargs.get("group_fields") or ""
        self.group_extended = kwargs.get("group_extended") or 0
        self.offset = kwargs.get('offset') or 0
        self.count = kwargs.get('count') or 0
        self.domain = kwargs['domain']

        self.url = f"{self.api_url}/method/groups.getById/?group_id={self.group_id}&v={self.api_version}&access_token={self.access_token}"
        self.members_url = f"{self.api_url}/method/groups.getMembers/?group_id={self.group_id}&v={self.api_version}&offset={self.offset}&access_token={self.access_token}"
        self.wall = f"{self.api_url}/method/wall.get/?group_id=-{self.group_id}&domain={self.domain}&offset={self.offset}&count={self.count}&v={self.api_version}&access_token={self.access_token}"

        super(VkGroupSpider, self).__init__(*args, **kwargs)

    def start_requests(self):
        yield scrapy.Request(self.url, self.parse, dont_filter=True)
        yield scrapy.Request(self.members_url, self.parse_members, dont_filter=True)
        yield scrapy.Request(self.wall, self.parse_wall, dont_filter=True)

    def parse(self, response, **kwargs):
        self.logger.info(f"parse group {self.group_id}")
        if "error" not in response.text:
            data = json.loads(response.text)
            if 'response' in data:
                group = data['response'][0]
                group["id"] = self.group_id
            else:
                group = {}
            return group

    def parse_members(self, response):
        self.logger.info(f"parse members {self.group_id}")
        if "error" not in response.text:
            data = json.loads(response.text)
            if 'response' in data:
                members = data['response']['items']
                yield {'members': members}

    def parse_wall(self, response):
        self.logger.info(f"parse wall {self.group_id}")
        if "error" not in response.text:
            data = json.loads(response.text)
            if 'response' in data:
                wall_posts = data['response']['items']
                posts = [{**x, 'text': x['text'].replace('\n\n', ' ').replace('  ', ' ')} for x in wall_posts]
                yield {'wall_posts': posts}
