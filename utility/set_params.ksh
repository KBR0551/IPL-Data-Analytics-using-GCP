#!/bin/bash
gsutil cp gs://BUCKET_NAME/config/.env /etc/profile.d/.env  #replace with your bucket name
echo "source /etc/profile.d/.env" >> /etc/profile
