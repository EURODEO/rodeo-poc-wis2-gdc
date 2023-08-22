###############################################################################
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
###############################################################################

import logging
import click
from urllib.parse import urlparse

from elasticsearch import Elasticsearch

from wis2_gdc.backend.base import BaseBackend
from wis2_gdc.env import COLLECTION_INDEX, EWC_URL, AWS_URL

LOGGER = logging.getLogger(__name__)


class ElasticsearchBackend(BaseBackend):

    from urllib.parse import urlparse
    from elasticsearch import Elasticsearch

    def __init__(self, defs):
        super().__init__(defs)

        # default index settings
        self.ES_SETTINGS = {
            'settings': {
                'number_of_shards': 1,
                'number_of_replicas': 0
            },
            'mappings': {
                'properties': {
                    'geometry': {
                        'type': 'geo_shape'
                    },
                    'id': {
                        'type': 'text',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    },
                    'properties': {
                        'properties': {
                            'type': {
                                'type': 'text',
                                'fields': {
                                    'raw': {
                                        'type': 'keyword'
                                    }
                                }
                            },
                            'title': {
                                'type': 'text',
                                'fields': {
                                    'raw': {
                                        'type': 'keyword'
                                    }
                                }
                            },
                            'description': {
                                'type': 'text',
                                'fields': {
                                    'raw': {
                                        'type': 'keyword'
                                    }
                                }
                            },
                            'wmo:topicHierarchy': {
                                'type': 'text',
                                'fields': {
                                    'raw': {
                                        'type': 'keyword'
                                    }
                                }
                            },
                            'wmo:dataPolicy': {
                                'type': 'text',
                                'fields': {
                                    'raw': {
                                        'type': 'keyword'
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        self.url_parsed = urlparse(self.defs.get('connection'))
        self.index_name = self.url_parsed.path.lstrip('/')
        self.collection_index = str(COLLECTION_INDEX)
        self.aws_url = str(AWS_URL)
        self.ewc_url = str(EWC_URL)

        url2 = f'{self.url_parsed.scheme}://{self.url_parsed.netloc}'

        if self.url_parsed.path.count('/') > 1:
            LOGGER.debug('ES URL has a basepath')
            basepath = self.url_parsed.path.split('/')[1]
            self.index_name = self.url_parsed.path.split('/')[-1]
            url2 = f'{url2}/{basepath}/'

        LOGGER.debug(f'ES URL: {url2}')
        LOGGER.debug(f'ES index: {self.index_name}')
        LOGGER.debug(f'ES collections index: {self.collection_index}')

        settings = {
            'hosts': [url2],
            'retry_on_timeout': True,
            'max_retries': 10,
            'timeout': 30
        }

        if self.url_parsed.username and self.url_parsed.password:
            settings['http_auth'] = (
                self.url_parsed.username, self.url_parsed.password)

        self.es = Elasticsearch(**settings)

    def setup(self) -> None:
        if self.es.indices.exists(index=self.index_name):
            LOGGER.debug(f'Deleting index {self.index_name}')
            self.es.indices.delete(index=self.index_name)
            self.es.indices.delete(index=self.collection_index)

        LOGGER.debug(f'Creating index {self.index_name}')
        self.es.indices.create(index=self.index_name, body=self.ES_SETTINGS)

        LOGGER.debug(f'Creating index {self.collection_index}')
        self.es.indices.create(index=self.collection_index)

    def save(self, record: dict) -> None:
        url_to_replace_with = ''
        enviroment = click.prompt(text='Type 1 for EWC and 2 AWS', type=click.IntRange(1,2))
        if enviroment is 1:
            url_to_replace_with = self.ewc_url
        elif enviroment is 2:
            url_to_replace_with = self.aws_url
        else:
            click.UsageError("Provide either value 1 or 2")

        LOGGER.debug(record)
        response = self.es.index(index=self.index_name, body=record)
        doc_id = response['_id']
        collections = []
        for link in record['links']:
            if link['rel'] == 'collection':
                # Add id linking back to the original metadata
                link['original_metadata_id'] = doc_id
                collection_response = self.es.index(index=self.collection_index, body=link)
                #Remove reference before updating original data
                link.pop('original_metadata_id')
                link['href'] = url_to_replace_with + '/' +  collection_response['_id']
                collections.append(link)
        record['links'] = collections
        # Save whole documented with updated links
        self.es.update(index=self.index_name, id=doc_id, body={"doc": record})


