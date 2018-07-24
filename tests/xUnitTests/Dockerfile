FROM microsoft/dotnet:2.1-sdk AS builder

# caches restore result by copying csproj file separately
COPY . ./app

WORKDIR /app/tests/xUnitTests

RUN dotnet restore
