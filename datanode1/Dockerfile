FROM python:3.7
COPY . /datanode1
EXPOSE 3002
RUN useradd -ms /bin/bash datanode1
RUN pip3 install --no-cache-dir -r /datanode1/requirements.txt
CMD python3 /datanode1/datanode.py