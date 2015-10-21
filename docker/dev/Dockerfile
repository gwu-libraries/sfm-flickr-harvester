FROM python:2.7
MAINTAINER Justin Littman <justinlittman@gwu.edu>

RUN pip install --upgrade pip
#This pre-fetches the most recent requirements.txt.
ADD https://github.com/gwu-libraries/sfm-flickr-harvester/raw/master/requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt
RUN pip install appdeps
WORKDIR /opt/sfm-flickr-harvester
CMD pip install -r requirements.txt --upgrade \
    && appdeps.py --port-wait mq:5672 \
    && python flickr_harvester.py service mq $MQ_ENV_RABBITMQ_DEFAULT_USER $MQ_ENV_RABBITMQ_DEFAULT_PASS