# sfm-flickr-harvester
A harvester for flickr content as part of [Social Feed Manager](https://gwu-libraries.github.io/sfm-ui).

[![Build Status](https://travis-ci.org/gwu-libraries/sfm-flickr-harvester.svg?branch=master)](https://travis-ci.org/gwu-libraries/sfm-flickr-harvester)

## Development

For information on development and running tests, see the [development documentation](http://sfm.readthedocs.io/en/latest/development.html).

When running tests, provide Flickr credentials either as a `test_config.py` file or environment variables (`FLICKR_KEY`,
`FLICKR_SECRET`).  An example `test_config.py` looks like:

    FLICKR_KEY = "acbdfdfe6b8bba356e8ef278ed65dbbc8"
    FLICKR_SECRET = "16264549fc54cc33eb"

## Running harvester as a service
Flickr harvester will act on harvest start messages received from a queue. To run as a service:

    python flickr_harvester.py service <mq host> <mq username> <mq password>
    
## Process harvest start files
Flickr harvester can process harvest start files. The format of a harvest start file is the same as a harvest start message.  To run:

    python flickr_harvester.py seed <path to file>

## Iterating over photo records in a WARC

    python flickr_photo_warc_iter.py <path to WARC>

## Running exporter as a service
Flickr exporter will act on export start messages received from a queue. To run as a service:

    python flickr_exporter.py service <mq host> <mq username> <mq password> <SFM UI REST API url>
    
## Process export start files
Flickr exporter can process export start files. The format of an export start file is the same as an export start message.  To run:

    python flickr_exporter.py file <path to file> <SFM UI REST API url>

## Harvest start messages
Following is information necessary to construct a harvest start message for the flickr harvester.

### User harvest type

Type: flickr_user

Api methods called:
  * people.findByUsername to get nsid if username provided
  * people.getInfo
  * people.getPublicPhotos
  * photos.getInfo for each photo

Required parameters:
  * username or nsid

Optional parameters:
  * incremental: True (default) or False
  * sizes:  List of [photo size labels](https://www.flickr.com/services/api/flickr.photos.getSizes.html).  Default is Thumbnail, Large, and Original.

Summary:
  * user
  * photo

Extracted urls: Urls are generated for each photo for each size.

### Authentication

Required parameters:
  * key
  * secret
