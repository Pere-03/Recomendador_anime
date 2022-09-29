FROM python:3.9.13-slim

ADD . ./

RUN /usr/local/bin/python -m pip install --upgrade pip && pip install -r requirements.txt

CMD ["python3", "recomendador_anime.py"]