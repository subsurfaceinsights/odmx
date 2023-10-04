FROM ubuntu:22.04 as odmx
RUN apt-get update && apt-get install -y \
  python3 python3-pip libpq5 uvicorn && \
  apt-get clean && rm -rf /var/lib/apt/lists/*
ARG PYLINT_VER
ARG PYTEST_VER
ARG BUILD_ENV
RUN if [ "$BUILD_ENV" = "dev" ]; then \
  pip3 install pylint==$PYLINT_VER pytest==$PYTEST_VER; \
fi
COPY requirements.txt /odmx/requirements.txt
WORKDIR /odmx
RUN pip3 install -r requirements.txt && rm -rf /root/.cache/pip
COPY . /odmx
RUN pip3 install -e .
ENTRYPOINT ["python3", "-m"]
CMD ["odmx"]

FROM odmx as odmx-api-v3
EXPOSE 8080
CMD ["uvicorn", "odmx.rest_api:app", "--host", "0.0.0.0", "--port", "8080"]
