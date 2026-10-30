FROM golang:1.25-bookworm AS server-build
WORKDIR /source
RUN git init && git remote add origin https://github.com/minio/minio.git \
    && git fetch --depth=1 origin 01ce918d8279a20e4706b96a64396146894adee4 \
    && git checkout --detach FETCH_HEAD \
    && CGO_ENABLED=0 go build -trimpath -o /out/minio .

FROM golang:1.25-bookworm AS client-build
WORKDIR /source
RUN git init && git remote add origin https://github.com/minio/mc.git \
    && git fetch --depth=1 origin d6541ea280b73a834b64d4097e21f2be77676104 \
    && git checkout --detach FETCH_HEAD \
    && CGO_ENABLED=0 go build -trimpath -o /out/mc .

FROM python:3.12-slim AS minio-server
COPY --from=server-build /out/minio /usr/local/bin/minio
COPY --from=server-build /source/LICENSE /usr/share/licenses/minio/LICENSE
RUN useradd --uid 10001 --create-home minio && mkdir /data && chown minio:minio /data
USER minio
ENTRYPOINT ["minio"]

FROM python:3.12-slim AS minio-client
COPY --from=client-build /out/mc /usr/local/bin/mc
COPY --from=client-build /source/LICENSE /usr/share/licenses/mc/LICENSE
ENTRYPOINT ["mc"]