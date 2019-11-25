# Group Project Assignment: Distributed File System

## Project setup
  - Install docker on your machine if it is not installed
  - Install postgres and run it's container on namenode
  + Run next commands:
     + For namenode:
  ```sudo docker run -i -t --network='host' deadman445/namenode:latest <your_namenode_ip> <your_namenode_host>```
     + For datanode:
  ```sudo docker run --network='host' -t deadman445/datanode:latest 10.0.15.10 <your_datanode_host> <your_datanode_ip> <your_namenode_host>```
  - Enjoy!  
## Architectural diagrams
![Chat](https://github.com/KonevDmitry/ds_project/blob/master/Untitled%20Diagram.jpg)

## Description of communication protocols
Communications protocols of our project use TCP connection for file transferring, sharing, creating and other file operations, which were required in project specification.
