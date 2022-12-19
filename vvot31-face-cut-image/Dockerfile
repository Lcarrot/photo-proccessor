FROM ubuntu

RUN apt update && apt install -y python3 python3-pip
RUN pip3 install sanic
RUN pip3 install Pillow
RUN pip3 install boto3
RUN pip3 install ydb
RUN pip3 install requests
COPY index.py /

CMD [ "python3", "/index.py" ]