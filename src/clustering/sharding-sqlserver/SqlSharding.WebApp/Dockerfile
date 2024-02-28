FROM mcr.microsoft.com/dotnet/aspnet:6.0
WORKDIR /app

EXPOSE 7918 7918
EXPOSE 5000 5000

COPY ./bin/Release/net6.0/publish/ /app

CMD ["dotnet", "SqlSharding.WebApp.dll"]