FROM python:3-alpine
RUN pip install DukeDSClient==2.1.1
ENV DDSCLIENT_CONF
CMD ddsclient
