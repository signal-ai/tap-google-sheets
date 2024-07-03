#!/usr/bin/env python3

import json
import sys

import singer

from tap_google_sheets.client import GoogleClient
from tap_google_sheets.discover import discover
from tap_google_sheets.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'spreadsheet_id',
    'start_date',
    'user_agent'
]

def do_discover(client, spreadsheet_id):

    LOGGER.info('Starting discover')
    catalog = discover(client, spreadsheet_id)
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info('Finished discover')


@singer.utils.handle_top_exception(LOGGER)
def main():

    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    with GoogleClient(client_id=parsed_args.config.get('client_id'),
                      client_secret=parsed_args.config.get('client_secret'),
                      refresh_token=parsed_args.config.get('refresh_token'),
                      request_timeout=parsed_args.config.get('request_timeout'),
                      user_agent=parsed_args.config['user_agent'],
                      api_key=parsed_args.config.get('api_key')
                      ) as client:

        state = {}
        if parsed_args.state:
            state = parsed_args.state

        config = parsed_args.config
        spreadsheet_id = config.get('spreadsheet_id')

        if parsed_args.discover:
            do_discover(client, spreadsheet_id)
        else:
            sync(client=client,
                 config=config,
                 catalog=parsed_args.catalog or discover(client, spreadsheet_id),
                 state=state)

if __name__ == '__main__':
    main()
