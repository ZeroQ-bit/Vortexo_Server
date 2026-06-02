FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY main.go ./
RUN go build -trimpath -ldflags="-s -w" -o /out/vortexo-manifest-server .

FROM alpine:3.20
RUN adduser -D -H -u 1000 vortexo
WORKDIR /app
COPY --from=build /out/vortexo-manifest-server /app/vortexo-manifest-server
ENV VORTEXO_LISTEN_ADDR=:8080
ENV VORTEXO_DATA_DIR=/data
EXPOSE 8080
VOLUME ["/data"]
USER 1000:1000
CMD ["/app/vortexo-manifest-server"]
