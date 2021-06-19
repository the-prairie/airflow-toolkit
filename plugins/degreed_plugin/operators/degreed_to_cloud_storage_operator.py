from tempfile import NamedTemporaryFile
from flatten_json import flatten
import logging
import json

from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.hooks import GoogleCloudStorageHook

from plugins.degreed_plugin.hooks.degreed_hook import DegreedHook


class DegreedToCloudStorageOperator(BaseOperator):
    """
    Github To Cloud Storage Operator
    :param degreed_conn_id:          The Degreed connection id.
    :type github_conn_id:            string
    :param degreed_object:            The desired Github object. The currently
                                     supported values are:
                                        - logins
                                        - users
                                        - completions
                                        - required-learning
                                        - views
    :type degreed_object:             string
    :param payload:                  The associated degreed parameters to
                                     pass into the object request as
                                     keyword arguments.
    :type payload:                   dict
    :param destination:              The final destination where the data
                                     should be stored. Possible values include:
                                        - GCS                           
    :type destination:               string
    :param dest_conn_id:             The destination connection id.
    :type dest_conn_id:              string
    :param bucket:                   The bucket to be used to store the data.
    :type bucket:                    string
    :param key:                      The filename to be used to store the data.
    :type key:                       string
    """


    template_field = ('key',)

    @apply_defaults
    def __init__(self,
                 degreed_conn_id,
                 degreed_object,
                 dest_conn_id,
                 bucket,
                 key,
                 destination='gcs',
                 payload={},
                 **kwargs):
        super().__init__(**kwargs)
        self.degreed_conn_id = degreed_conn_id
        self.degreed_object = degreed_object
        self.payload = payload
        self.destination = destination
        self.dest_conn_id = dest_conn_id
        self.bucket = bucket
        self.key = key

        if self.degreed_object.lower() not in ('logins',
                                              'users',
                                              'completions',
                                              'views',
                                              'required-learning'):
            raise Exception('Specified Degreed object not currently supported.')

    def execute(self, context):
        g = DegreedHook(self.degreed_conn_id)
        output = []
