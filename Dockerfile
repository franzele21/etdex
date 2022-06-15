#syntax=docker/dockerfile:1

FROM python:latest

WORKDIR /etdex

COPY . /etdex

RUN pip install -r requirements.txt

CMD [ "./run.sh" ]