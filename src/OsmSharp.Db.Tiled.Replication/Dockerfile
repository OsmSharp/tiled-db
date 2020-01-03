FROM mcr.microsoft.com/dotnet/core/sdk:3.1

# File Author / Maintainer
MAINTAINER ANYWAYS BVBA

# copy api assemblies and files
RUN mkdir /var/app
RUN mkdir /var/app/source
RUN mkdir /var/app/target
RUN mkdir /var/app/logs

# install cron.
RUN apt-get update
RUN apt-get install -y cron
ADD docker-crontab /
RUN crontab /docker-crontab

COPY ./bin/release/netcoreapp3.1/publish /var/app
# COPY config.Docker.json /var/app/config.json

# couple data folder data volume.
VOLUME ["/var/app/source"]
VOLUME ["/var/app/target"]
VOLUME ["/var/app/logs"]

# Set the default command to execute when creating a new container
WORKDIR /var/app/
ENTRYPOINT cron -f