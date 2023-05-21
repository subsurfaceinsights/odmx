FROM ubuntu:22.04 as odmx
RUN apt-get update && apt-get install -y python3 python3-pip libpq5
COPY requirements.txt /odmx/requirements.txt
WORKDIR /odmx
RUN pip3 install -r requirements.txt
COPY . /odmx
RUN pip3 install -e .
ENTRYPOINT ["python3", "-m"]
CMD ["odmx"]
