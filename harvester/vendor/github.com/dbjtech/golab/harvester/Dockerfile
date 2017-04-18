FROM golang
ADD . /go/src/github.com/dbjtech/golab
RUN go install github.com/dbjtech/golab/harvester
EXPOSE 2379 2380
ENTRYPOINT ["harvester"]
