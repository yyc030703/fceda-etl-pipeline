FROM public.ecr.aws/lambda/python:3.9

RUN yum install -y \
    gcc \
    gdal gdal-devel \
    proj proj-devel \
    geos geos-devel \
    && yum clean all

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

CMD ["app.lambda_handler"]
