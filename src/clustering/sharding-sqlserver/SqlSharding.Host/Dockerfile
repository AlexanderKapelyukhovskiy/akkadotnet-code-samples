#See https://aka.ms/customizecontainer to learn how to customize your debug container and how Visual Studio uses this Dockerfile to build your images for faster debugging.

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS base
WORKDIR /app

# should be a comma-delimited list
ENV CLUSTER_SEEDS "[]"
ENV CLUSTER_IP ""
ENV CLUSTER_PORT "5055"
ENV MONGO_CONNECTION_STR "" #MongoDb connection string for Akka.Persistence

# 9110 - Petabridge.Cmd
# 5055 - Akka.Cluster
EXPOSE 9110 5055

# Install Petabridge.Cmd client
RUN dotnet tool install --global pbm --version 1.3.0

COPY ./bin/Release/net6.0/publish/ /app

FROM mcr.microsoft.com/dotnet/aspnet:6.0 AS app
WORKDIR /app

COPY --from=base /app /app

# copy .NET Core global tool
COPY --from=base /root/.dotnet /root/.dotnet/

# Needed because https://stackoverflow.com/questions/51977474/install-dotnet-core-tool-dockerfile
ENV PATH="${PATH}:/root/.dotnet/tools"

# RUN pbm help

CMD ["dotnet", "SqlSharding.Host.dll"]