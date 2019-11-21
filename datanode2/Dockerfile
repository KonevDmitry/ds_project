FROM python:3.7
COPY . /datanode2
EXPOSE 3002
RUN useradd -ms /bin/bash datanode2
RUN pip3 install --no-cache-dir -r /datanode2/requirements.txt
CMD python3 /datanode2/datanode.py