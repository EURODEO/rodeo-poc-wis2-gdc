FROM docker.io/python:3.11-alpine


WORKDIR /myapp

#RUN apk add --no-cache python3-dev build-base
#RUN apk add --no-cache python3 py3-pip && \
RUN apk add gcc libc-dev geos-dev geos bash && \
    python3 -m venv --system-site-packages venv \
    mkdir /myapp/venv/wis2-gdc

SHELL ["/bin/bash", "-c"] 

COPY . /myapp/venv/wis2-gdc
RUN cd /myapp/venv && \
    source ./bin/activate && \
    cd wis2-gdc && \
    python3 setup.py install && \
    python3 -m pip install elasticsearch
ENV VIRTUAL_ENV=/"/myapp/venv"
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

ENV WIS2_GDC_API_URL=http://localhost:5000
ENV WIS2_GDC_BACKEND=Elasticsearch
ENV WIS2_GDC_CONNECTION=http://elasticsearch:9200/wis2-discovery-metadata
ENV WIS2_GDC_COLLECTION_INDEX=metadata_collection

CMD [ "/bin/bash"]
