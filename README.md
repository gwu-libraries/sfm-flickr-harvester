# sfm-flickr-harvester
A harvester for flickr content as part of Social Feed Manager

## Installing
    git clone https://github.com/gwu-libraries/sfm-flickr-harvester.git
    cd sfm-flickr-harvester
    pip install -r requirements.txt

## Running as a service
Flickr harvester will act on harvest start messages received from a queue. To run as a service:

    python flickr_harvester.py service <mq host> <mq username> <mq password>
    
## Process harvest start files
Flickr harvester can process harvest start files. The format of a harvest start file is the same as a harvest start message.  To run:

    python flickr_harvester.py seed <path to file>

## Tests
For all tests:

* Get a [flickr api key](https://www.flickr.com/services/api/misc.api_keys.html).
* Provide the key and secret to the tests. This can be done either by putting them in a file named `test_config.py`
or in environment variables (`FLICKR_KEY` and `FLICKR_SECRET`).  An example `test_config.py` looks like:

    FLICKR_KEY = "acbdfdfe6b8bba356e8ef278ed65dbbc8"
    FLICKR_SECRET = "16264549fc54cc33eb"


### Unit tests
    python -m unittest discover

### Integration tests (inside docker containers)
1. Install [Docker](https://docs.docker.com/installation/) and [Docker-Compose](https://docs.docker.com/compose/install/).
2. Start up the containers.

        docker-compose -f docker/dev.docker-compose.yml up -d

3. Run the tests.

        docker exec docker_sfmflickrharvester_1 python -m unittest discover

4. Shutdown containers.

        docker-compose -f docker/dev.docker-compose.yml kill
        docker-compose -f docker/dev.docker-compose.yml rm -v --force
        

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