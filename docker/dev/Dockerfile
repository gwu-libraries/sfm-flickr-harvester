FROM python@sha256:ad39551743b356efda7c61f46019b97d49d1aab01b97f0e6d87c9b34326f3bfe
#Python 2.7
MAINTAINER Justin Littman <justinlittman@gwu.edu>

RUN apt-get update && apt-get install -y \
    git=1:2.1.4-2.1+deb8u1
RUN pip install pip==7.1.2
#This pre-fetches the most recent requirements.txt.
ADD https://raw.githubusercontent.com/gwu-libraries/sfm-flickr-harvester/master/requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt
RUN pip install appdeps
#This is temporary until it is in master requirements.txt.
RUN pip install librabbitmq
RUN pip install git+https://github.com/gwu-libraries/warcprox.git@0.1.0#egg=warcprox-gwu
WORKDIR /opt/sfm-flickr-harvester
#This links in sfm-utils as develop.
CMD cd /opt/sfm-utils \
    && python setup.py develop \
    && cd /opt/sfm-flickr-harvester \
    && pip install -r requirements.txt --upgrade \
    && appdeps.py --port-wait mq:5672 \
    && python flickr_harvester.py --debug=$DEBUG service mq $MQ_ENV_RABBITMQ_DEFAULT_USER $MQ_ENV_RABBITMQ_DEFAULT_PASS
