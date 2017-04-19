FROM golang
ADD . /go/src/github.com/dbjtech/golab/harvester
WORKDIR /go/src/github.com/dbjtech/golab/harvester
RUN go install github.com/dbjtech/golab/harvester
ENTRYPOINT ["harvester"]
