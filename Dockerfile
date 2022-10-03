FROM 10.3.10.127:5050/1582/apg_md_aps_schedule/tf-2.5.0:v5
LABEL version="0.1"
WORKDIR /usr/src/app
COPY MD_data_crawler/ /usr/src/app/
