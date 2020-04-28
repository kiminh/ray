FROM python:3.7
RUN pip install kopf
RUN pip install kubernetes
COPY controller/ /controller
COPY controller/handlers.py /handlers.py
CMD kopf run --standalone /handlers.py