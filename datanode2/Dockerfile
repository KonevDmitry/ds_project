FROM python:3.7
COPY . /datanode
EXPOSE 4000
RUN useradd -ms /bin/bash datanode
RUN pip3 install --no-cache-dir -r /datanode/requirements.txt
CMD python3 /datanode/datanode.py