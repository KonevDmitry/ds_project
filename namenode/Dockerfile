FROM python:3.7
COPY . /namenode
EXPOSE 3002
RUN useradd -ms /bin/bash namenode
RUN pip3 install --no-cache-dir -r /namenode/requirements.txt
CMD python3 /namenode/namenode.py