#!/bin/bash
gsutil cp gs://vocal-chiller-457916-r2/config/.env /etc/profile.d/.env
echo "source /etc/profile.d/.env" >> /etc/profile