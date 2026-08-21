ARG GO_VERSION=1.25
FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/quicshot ./cmd/quicshot

FROM alpine:3.22
ARG QUICshot_REVISION=dev
LABEL quicshot.revision=$QUICshot_REVISION
# iproute2 provides tc/netem for impairment; iputils and curl help manual poking.
RUN apk add --no-cache iproute2 curl ca-certificates \
	&& adduser -D -H -u 10001 quicshot
COPY --from=build /out/quicshot /usr/local/bin/quicshot
USER quicshot
ENTRYPOINT ["quicshot"]
