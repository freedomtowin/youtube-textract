FROM python:3.10-bullseye

ARG BUILD_ENV_ARG=prd
ENV BUILD_ENV=$BUILD_ENV_ARG

# ARG OPENAI_API_KEY_ARG=NA
# ARG AWS_ACCESS_KEY_ID_ARG=NA
# ARG OPENAI_API_KEY_ARG=NA

ENV OPENAI_API_KEY=$OPENAI_API_KEY_ARG
ENV AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID_ARG
ENV OPENAI_API_KEY=$OPENAI_API_KEY_ARG

RUN mkdir /root/working

COPY utils /root/utils/
COPY models /root/models/
COPY data /root/data/
COPY audio /root/audio/
COPY app.py /root/app.py

COPY entrypoint.sh /root/entrypoint.sh
RUN chmod +x /root/entrypoint.sh

COPY aws_setup.sh /root/aws_setup.sh
RUN chmod +x /root/aws_setup.sh


WORKDIR /root/models/modules/
RUN python setup.py install

WORKDIR /root/



RUN apt-get update -y && apt update -y
RUN apt-get install -y libsndfile-dev
RUN apt install ffmpeg -y

# files
RUN pip install --upgrade pip
RUN pip install transformers sentencepiece torch boto3 Flask Flask-Cors 
RUN pip install pandas pyarrow awswrangler scipy openai langchain lancedb 
RUN pip install bs4
RUN pip install presidio-analyzer>=2.2.33 presidio-anonymizer>=2.2.33 pydantic>=1.10.9 lxml>=4.9.2 spacy>=3.5.4 tiktoken>=0.3.3
RUN pip install --force-reinstall https://github.com/yt-dlp/yt-dlp/archive/master.tar.gz


RUN ["/bin/bash", "aws_setup.sh"]
RUN rm /root/aws_setup.sh

RUN apt-get clean \
    && rm -rf /var/lib/apt/lists/



# EXPOSE 50051
EXPOSE 80


ENTRYPOINT  ["/bin/bash", "entrypoint.sh"]
CMD ["sleep","infinity"]