#syntax=docker/dockerfile:1

FROM python:3.10

WORKDIR /etdex

COPY . /etdex

RUN pip install -r requirements.txt

CMD [ "./run.sh" ]