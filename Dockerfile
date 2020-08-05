FROM python:3.8

RUN pip install pipenv

ADD lib /
ADD config.yaml /
ADD Pipfile /
ADD Pipfile.lock /
ADD main.py /

RUN pipenv sync

CMD ["python", "./main.py"]
