FROM python:3.8-slim-buster

EXPOSE 8501

RUN apt-get update && apt-get install -y \
    build-essential \
    software-properties-common \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /snowflake_streamlit_app

COPY . /snowflake_streamlit_app

COPY entrypoint.sh /entrypoint.sh

RUN pip install -r requirements.txt
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]